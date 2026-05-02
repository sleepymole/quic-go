package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

const (
	bbrv3StartupPacingGain = 2.885
	bbrv3DrainPacingGain   = 1 / bbrv3StartupPacingGain
	bbrv3ProbeDownGain     = 0.9
	bbrv3ProbeCruiseGain   = 1.0
	bbrv3ProbeRefillGain   = 1.0
	bbrv3ProbeUpGain       = 1.25

	bbrv3StartupCwndGain = 2.0
	bbrv3CwndGain        = 2.0

	bbrv3FullBandwidthRounds = 3
	bbrv3FullBandwidthGain   = 1.25
	bbrv3MinRTTWindow        = time.Minute
	bbrv3ProbeRTTDuration    = 200 * time.Millisecond
	bbrv3LossThreshold       = 0.20

	bbrv3InitialCongestionWindowPackets = 128
	bbrv3MinCongestionWindowPackets     = 32
	bbrv3PacingMargin                   = 1.25
)

type bbrv3Mode uint8

const (
	bbrv3Startup bbrv3Mode = iota
	bbrv3Drain
	bbrv3ProbeBWDown
	bbrv3ProbeBWCruise
	bbrv3ProbeBWRefill
	bbrv3ProbeBWUp
	bbrv3ProbeRTT
)

func (m bbrv3Mode) String() string {
	switch m {
	case bbrv3Startup:
		return "bbrv3_startup"
	case bbrv3Drain:
		return "bbrv3_drain"
	case bbrv3ProbeBWDown:
		return "bbrv3_probe_bw_down"
	case bbrv3ProbeBWCruise:
		return "bbrv3_probe_bw_cruise"
	case bbrv3ProbeBWRefill:
		return "bbrv3_probe_bw_refill"
	case bbrv3ProbeBWUp:
		return "bbrv3_probe_bw_up"
	case bbrv3ProbeRTT:
		return "bbrv3_probe_rtt"
	default:
		return "bbrv3_unknown"
	}
}

// bbrv3Sender implements an experimental BBRv3-style model-based congestion
// controller. It follows the BBRv3 state structure and loss-aware inflight
// bounding, while keeping the implementation local to quic-go's existing
// sender interface.
type bbrv3Sender struct {
	clock     Clock
	rttStats  *utils.RTTStats
	connStats *utils.ConnectionStats
	pacer     *pacer
	qlogger   qlogwriter.Recorder

	mode       bbrv3Mode
	pacingGain float64
	cwndGain   float64

	maxDatagramSize  protocol.ByteCount
	congestionWindow protocol.ByteCount
	pacingRate       Bandwidth
	maxBandwidth     Bandwidth

	minRTT          time.Duration
	minRTTTimestamp monotime.Time

	nextRoundDelivered protocol.ByteCount
	roundStart         bool

	fullBandwidth      Bandwidth
	fullBandwidthCount int
	fullBandwidthFound bool

	lostBytesInRound protocol.ByteCount

	probeRTTDone  monotime.Time
	probeBWStamp  monotime.Time
	totalAcked    protocol.ByteCount
	lastStateName qlog.CongestionState
}

var (
	_ SendAlgorithm               = &bbrv3Sender{}
	_ SendAlgorithmWithDebugInfos = &bbrv3Sender{}
	_ RateSampleAware             = &bbrv3Sender{}
)

// NewBBRv3Sender creates an experimental BBRv3 sender.
func NewBBRv3Sender(
	clock Clock,
	rttStats *utils.RTTStats,
	connStats *utils.ConnectionStats,
	initialMaxDatagramSize protocol.ByteCount,
	qlogger qlogwriter.Recorder,
) SendAlgorithmWithDebugInfos {
	b := &bbrv3Sender{
		clock:            clock,
		rttStats:         rttStats,
		connStats:        connStats,
		qlogger:          qlogger,
		mode:             bbrv3Startup,
		pacingGain:       bbrv3StartupPacingGain,
		cwndGain:         bbrv3StartupCwndGain,
		maxDatagramSize:  initialMaxDatagramSize,
		congestionWindow: bbrv3InitialCongestionWindowPackets * initialMaxDatagramSize,
	}
	b.pacingRate = bandwidthTimesGain(b.initialPacingRate(), bbrv3StartupPacingGain)
	b.pacer = newPacerWithAdjustedBandwidth(func() uint64 {
		rate := b.pacingRate
		if rate == 0 {
			rate = b.initialPacingRate()
		}
		bytesPerSecond := uint64(rate / BytesPerSecond)
		if bytesPerSecond == 0 {
			return uint64(initialMaxDatagramSize)
		}
		return uint64(float64(bytesPerSecond) * bbrv3PacingMargin)
	})
	b.maybeQlogStateChange(b.mode)
	return b
}

func (b *bbrv3Sender) TimeUntilSend(_ protocol.ByteCount) monotime.Time {
	return b.pacer.TimeUntilSend()
}

func (b *bbrv3Sender) HasPacingBudget(now monotime.Time) bool {
	return b.pacer.Budget(now) >= b.maxDatagramSize
}

func (b *bbrv3Sender) OnPacketSent(
	sentTime monotime.Time,
	_ protocol.ByteCount,
	_ protocol.PacketNumber,
	bytes protocol.ByteCount,
	isRetransmittable bool,
) {
	b.pacer.SentPacket(sentTime, bytes)
	if !isRetransmittable {
		return
	}
	if b.probeBWStamp.IsZero() {
		b.probeBWStamp = sentTime
	}
}

func (b *bbrv3Sender) CanSend(bytesInFlight protocol.ByteCount) bool {
	return bytesInFlight < b.GetCongestionWindow()
}

func (b *bbrv3Sender) InSlowStart() bool {
	return b.mode == bbrv3Startup
}

func (b *bbrv3Sender) InRecovery() bool {
	return false
}

func (b *bbrv3Sender) GetCongestionWindow() protocol.ByteCount {
	return b.congestionWindow
}

func (b *bbrv3Sender) MaybeExitSlowStart() {}

func (b *bbrv3Sender) OnPacketAcked(
	_ protocol.PacketNumber,
	ackedBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
	eventTime monotime.Time,
) {
	interval := b.rttStats.SmoothedRTT()
	if interval <= 0 {
		interval = protocol.TimerGranularity
	}
	sample := RateSample{
		Time:           eventTime,
		PriorDelivered: b.totalAcked,
		Delivered:      ackedBytes,
		Interval:       interval,
		RTT:            b.rttStats.LatestRTT(),
		PriorInFlight:  priorInFlight,
		BytesInFlight:  priorInFlight - min(priorInFlight, ackedBytes),
		TotalDelivered: b.totalAcked + ackedBytes,
	}
	b.OnPacketAckedWithRateSample(ackedBytes, sample)
}

func (b *bbrv3Sender) OnPacketAckedWithRateSample(
	ackedBytes protocol.ByteCount,
	sample RateSample,
) {
	if ackedBytes == 0 {
		return
	}
	b.totalAcked += ackedBytes
	b.updateRound(sample)
	if b.roundStart {
		b.checkLossRound(sample)
	}
	b.updateModel(sample)
	b.updateFullBandwidth()
	b.updateMode(sample)
	b.updateCongestionWindow(ackedBytes)
	b.updatePacingRate()
}

func (b *bbrv3Sender) OnCongestionEvent(
	_ protocol.PacketNumber,
	lostBytes protocol.ByteCount,
	priorInFlight protocol.ByteCount,
) {
	if lostBytes == 0 {
		return
	}
	b.connStats.PacketsLost.Add(1)
	b.connStats.BytesLost.Add(uint64(lostBytes))
	b.lostBytesInRound += lostBytes
	if priorInFlight == 0 {
		return
	}
	lossRate := float64(b.lostBytesInRound) / float64(priorInFlight)
	if lossRate >= bbrv3LossThreshold && b.mode == bbrv3ProbeBWUp {
		b.enterProbeBW(bbrv3ProbeBWDown, b.clock.Now())
	}
}

func (b *bbrv3Sender) OnRetransmissionTimeout(packetsRetransmitted bool) {
	if !packetsRetransmitted {
		return
	}
	b.fullBandwidthFound = false
	b.fullBandwidthCount = 0
	b.maxBandwidth = 0
	b.pacingRate = b.initialPacingRate()
	b.congestionWindow = b.minCongestionWindow()
	b.enterMode(bbrv3Startup)
}

func (b *bbrv3Sender) SetMaxDatagramSize(s protocol.ByteCount) {
	if s < b.maxDatagramSize {
		return
	}
	cwndIsMin := b.congestionWindow == b.minCongestionWindow()
	b.maxDatagramSize = s
	if cwndIsMin {
		b.congestionWindow = b.minCongestionWindow()
	}
	b.pacer.SetMaxDatagramSize(s)
}

func (b *bbrv3Sender) updateRound(sample RateSample) {
	b.roundStart = false
	if sample.PriorDelivered >= b.nextRoundDelivered {
		b.roundStart = true
		b.nextRoundDelivered = sample.TotalDelivered
	}
}

func (b *bbrv3Sender) updateModel(sample RateSample) {
	if sample.Interval > 0 && sample.Delivered > 0 {
		bw := BandwidthFromDelta(sample.Delivered, sample.Interval)
		b.maxBandwidth = max(b.maxBandwidth, bw)
	}
	rtt := sample.RTT
	if rtt <= 0 {
		rtt = b.rttStats.LatestRTT()
	}
	if rtt <= 0 {
		return
	}
	if b.minRTT == 0 || rtt < b.minRTT {
		b.minRTT = rtt
		b.minRTTTimestamp = sample.Time
	}
}

func (b *bbrv3Sender) updateFullBandwidth() {
	if b.fullBandwidthFound || !b.roundStart || b.maxBandwidth == 0 {
		return
	}
	if b.fullBandwidth == 0 || b.maxBandwidth >= bandwidthTimesGain(b.fullBandwidth, bbrv3FullBandwidthGain) {
		b.fullBandwidth = b.maxBandwidth
		b.fullBandwidthCount = 0
		return
	}
	b.fullBandwidthCount++
	if b.fullBandwidthCount >= bbrv3FullBandwidthRounds {
		b.fullBandwidthFound = true
	}
}

func (b *bbrv3Sender) updateMode(sample RateSample) {
	now := sample.Time
	if b.shouldEnterProbeRTT(now) {
		b.enterMode(bbrv3ProbeRTT)
		b.probeRTTDone = 0
		if sample.BytesInFlight <= b.minCongestionWindow() {
			b.probeRTTDone = now.Add(bbrv3ProbeRTTDuration)
		}
		return
	}
	if b.mode == bbrv3ProbeRTT {
		if b.probeRTTDone.IsZero() && sample.BytesInFlight <= b.minCongestionWindow() {
			b.probeRTTDone = now.Add(bbrv3ProbeRTTDuration)
			return
		}
		if !b.probeRTTDone.IsZero() && !now.Before(b.probeRTTDone) {
			b.minRTTTimestamp = now
			if b.fullBandwidthFound {
				b.enterProbeBW(bbrv3ProbeBWCruise, now)
			} else {
				b.enterMode(bbrv3Startup)
			}
		}
		return
	}
	switch b.mode {
	case bbrv3Startup:
		if b.fullBandwidthFound {
			b.enterMode(bbrv3Drain)
		}
	case bbrv3Drain:
		if sample.BytesInFlight <= b.targetInflight(1.0) {
			b.enterProbeBW(bbrv3ProbeBWCruise, now)
		}
	case bbrv3ProbeBWDown, bbrv3ProbeBWCruise, bbrv3ProbeBWRefill, bbrv3ProbeBWUp:
		b.advanceProbeBW(now)
	}
}

func (b *bbrv3Sender) updateCongestionWindow(ackedBytes protocol.ByteCount) {
	target := b.targetInflight(b.cwndGain)
	if b.mode == bbrv3ProbeRTT {
		target = b.minCongestionWindow()
	}
	if b.congestionWindow < target || b.totalAcked < b.initialCongestionWindow() {
		b.congestionWindow += ackedBytes
		if b.congestionWindow > target {
			b.congestionWindow = target
		}
	} else if b.mode == bbrv3ProbeRTT && b.congestionWindow > target {
		b.congestionWindow = target
	}
	if minCwnd := b.minCongestionWindow(); b.congestionWindow < minCwnd {
		b.congestionWindow = minCwnd
	}
}

func (b *bbrv3Sender) updatePacingRate() {
	bw := b.maxBandwidth
	if bw == 0 {
		bw = b.initialPacingRate()
	}
	b.pacingRate = bandwidthTimesGain(bw, b.pacingGain)
	if initial := b.initialPacingRate(); b.pacingRate < initial {
		b.pacingRate = initial
	}
}

func (b *bbrv3Sender) checkLossRound(sample RateSample) {
	if b.lostBytesInRound == 0 {
		return
	}
	denominator := sample.PriorInFlight
	if denominator == 0 {
		denominator = b.targetInflight(1.0)
	}
	if denominator > 0 && float64(b.lostBytesInRound)/float64(denominator) >= bbrv3LossThreshold && b.mode == bbrv3ProbeBWUp {
		b.enterProbeBW(bbrv3ProbeBWDown, sample.Time)
	}
	b.lostBytesInRound = 0
}

func (b *bbrv3Sender) shouldEnterProbeRTT(now monotime.Time) bool {
	if b.minRTT == 0 || b.minRTTTimestamp.IsZero() || b.mode == bbrv3ProbeRTT || b.mode == bbrv3Startup {
		return false
	}
	return now.Sub(b.minRTTTimestamp) > bbrv3MinRTTWindow
}

func (b *bbrv3Sender) advanceProbeBW(now monotime.Time) {
	interval := b.minRTT
	if interval <= 0 {
		interval = b.rttStats.SmoothedRTT()
	}
	if interval <= 0 || now.Sub(b.probeBWStamp) < interval {
		return
	}
	switch b.mode {
	case bbrv3ProbeBWDown:
		b.enterProbeBW(bbrv3ProbeBWCruise, now)
	case bbrv3ProbeBWCruise:
		b.enterProbeBW(bbrv3ProbeBWRefill, now)
	case bbrv3ProbeBWRefill:
		b.enterProbeBW(bbrv3ProbeBWUp, now)
	case bbrv3ProbeBWUp:
		b.enterProbeBW(bbrv3ProbeBWDown, now)
	}
}

func (b *bbrv3Sender) enterProbeBW(mode bbrv3Mode, now monotime.Time) {
	b.enterMode(mode)
	b.probeBWStamp = now
}

func (b *bbrv3Sender) enterMode(mode bbrv3Mode) {
	if b.mode == mode {
		return
	}
	b.mode = mode
	switch mode {
	case bbrv3Startup:
		b.pacingGain = bbrv3StartupPacingGain
		b.cwndGain = bbrv3StartupCwndGain
	case bbrv3Drain:
		b.pacingGain = bbrv3DrainPacingGain
		b.cwndGain = bbrv3CwndGain
	case bbrv3ProbeBWDown:
		b.pacingGain = bbrv3ProbeDownGain
		b.cwndGain = bbrv3CwndGain
	case bbrv3ProbeBWCruise:
		b.pacingGain = bbrv3ProbeCruiseGain
		b.cwndGain = bbrv3CwndGain
	case bbrv3ProbeBWRefill:
		b.pacingGain = bbrv3ProbeRefillGain
		b.cwndGain = bbrv3CwndGain
	case bbrv3ProbeBWUp:
		b.pacingGain = bbrv3ProbeUpGain
		b.cwndGain = bbrv3CwndGain
	case bbrv3ProbeRTT:
		b.pacingGain = bbrv3ProbeCruiseGain
		b.cwndGain = 1.0
	}
	b.maybeQlogStateChange(mode)
}

func (b *bbrv3Sender) targetInflight(gain float64) protocol.ByteCount {
	if b.maxBandwidth == 0 || b.minRTT <= 0 {
		return b.initialCongestionWindow()
	}
	bytesPerSecond := uint64(b.maxBandwidth / BytesPerSecond)
	bdp := protocol.ByteCount(bytesPerSecond * uint64(b.minRTT) / uint64(time.Second))
	target := bytesTimesGain(bdp, gain)
	if minCwnd := b.minCongestionWindow(); target < minCwnd {
		return minCwnd
	}
	return target
}

func (b *bbrv3Sender) initialPacingRate() Bandwidth {
	rtt := b.rttStats.SmoothedRTT()
	if rtt <= 0 {
		rtt = utils.DefaultInitialRTT
	}
	return BandwidthFromDelta(b.initialCongestionWindow(), rtt)
}

func (b *bbrv3Sender) initialCongestionWindow() protocol.ByteCount {
	return bbrv3InitialCongestionWindowPackets * b.maxDatagramSize
}

func (b *bbrv3Sender) minCongestionWindow() protocol.ByteCount {
	return bbrv3MinCongestionWindowPackets * b.maxDatagramSize
}

func (b *bbrv3Sender) maybeQlogStateChange(mode bbrv3Mode) {
	if b.qlogger == nil {
		return
	}
	state := qlog.CongestionState(mode.String())
	if state == b.lastStateName {
		return
	}
	b.qlogger.RecordEvent(qlog.CongestionStateUpdated{State: state})
	b.lastStateName = state
}

func bytesTimesGain(v protocol.ByteCount, gain float64) protocol.ByteCount {
	return protocol.ByteCount(float64(v) * gain)
}

func bandwidthTimesGain(v Bandwidth, gain float64) Bandwidth {
	return Bandwidth(float64(v) * gain)
}
