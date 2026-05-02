package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"

	"github.com/stretchr/testify/require"
)

type testBBRv3Sender struct {
	sender    *bbrv3Sender
	clock     *mockClock
	delivered protocol.ByteCount
}

func newTestBBRv3Sender() *testBBRv3Sender {
	var clock mockClock
	rttStats := utils.NewRTTStats()
	rttStats.UpdateRTT(100*time.Millisecond, 0)
	sender := NewBBRv3Sender(&clock, rttStats, &utils.ConnectionStats{}, maxDatagramSize, nil).(*bbrv3Sender)
	return &testBBRv3Sender{
		sender: sender,
		clock:  &clock,
	}
}

func (s *testBBRv3Sender) Ack(delivered protocol.ByteCount, interval time.Duration) {
	s.clock.Advance(interval)
	priorDelivered := s.delivered
	s.delivered += delivered
	s.sender.OnPacketAckedWithRateSample(delivered, RateSample{
		Time:           s.clock.Now(),
		PriorDelivered: priorDelivered,
		TotalDelivered: s.delivered,
		Delivered:      delivered,
		Interval:       interval,
		RTT:            100 * time.Millisecond,
		PriorInFlight:  s.sender.GetCongestionWindow(),
		BytesInFlight:  0,
	})
}

func TestNewSendAlgorithmCongestionControlSelection(t *testing.T) {
	rttStats := utils.NewRTTStats()
	connStats := &utils.ConnectionStats{}

	require.IsType(t, &cubicSender{}, NewSendAlgorithm(CongestionControlReno, rttStats, connStats, maxDatagramSize, nil))
	require.IsType(t, &cubicSender{}, NewSendAlgorithm(CongestionControlCubic, rttStats, connStats, maxDatagramSize, nil))
	require.IsType(t, &bbrv3Sender{}, NewSendAlgorithm(CongestionControlBBRv3, rttStats, connStats, maxDatagramSize, nil))
}

func TestBBRv3SenderLeavesStartupForProbeBW(t *testing.T) {
	s := newTestBBRv3Sender()

	require.Equal(t, bbrv3Startup, s.sender.mode)
	for range bbrv3FullBandwidthRounds + 2 {
		s.Ack(16*maxDatagramSize, 100*time.Millisecond)
	}

	require.Equal(t, bbrv3ProbeBWCruise, s.sender.mode)
	require.True(t, s.sender.fullBandwidthFound)
	require.NotZero(t, s.sender.maxBandwidth)
}

func TestBBRv3SenderBoundsInflightOnHighLoss(t *testing.T) {
	s := newTestBBRv3Sender()
	s.sender.enterProbeBW(bbrv3ProbeBWUp, s.clock.Now())
	priorInFlight := 100 * maxDatagramSize

	s.sender.OnCongestionEvent(1, 3*maxDatagramSize, priorInFlight)

	require.Equal(t, bbrv3ProbeBWDown, s.sender.mode)
	require.Less(t, s.sender.inflightHigh, priorInFlight)
	require.GreaterOrEqual(t, s.sender.inflightHigh, s.sender.minCongestionWindow())
}

func TestBBRv3SenderProbeRTT(t *testing.T) {
	s := newTestBBRv3Sender()
	s.Ack(16*maxDatagramSize, 100*time.Millisecond)
	s.sender.fullBandwidthFound = true
	s.sender.enterProbeBW(bbrv3ProbeBWCruise, s.clock.Now())
	s.clock.Advance(bbrv3MinRTTWindow + time.Millisecond)

	s.Ack(maxDatagramSize, 100*time.Millisecond)
	require.Equal(t, bbrv3ProbeRTT, s.sender.mode)

	s.Ack(maxDatagramSize, bbrv3ProbeRTTDuration+time.Millisecond)
	require.Equal(t, bbrv3ProbeBWCruise, s.sender.mode)
}
