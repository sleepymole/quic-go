package congestion

import (
	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/internal/utils"
	"github.com/quic-go/quic-go/qlogwriter"
)

const (
	// CongestionControlReno preserves the historical quic-go default.
	CongestionControlReno = "reno"
	// CongestionControlCubic selects Cubic congestion control.
	CongestionControlCubic = "cubic"
	// CongestionControlBBRv3 selects experimental BBRv3 congestion control.
	CongestionControlBBRv3 = "bbrv3"
)

// NewSendAlgorithm builds the requested congestion controller.
func NewSendAlgorithm(
	name string,
	rttStats *utils.RTTStats,
	connStats *utils.ConnectionStats,
	initialMaxDatagramSize protocol.ByteCount,
	qlogger qlogwriter.Recorder,
) SendAlgorithmWithDebugInfos {
	switch name {
	case CongestionControlReno:
		return NewCubicSender(
			DefaultClock{},
			rttStats,
			connStats,
			initialMaxDatagramSize,
			true,
			qlogger,
		)
	case CongestionControlCubic:
		return NewCubicSender(
			DefaultClock{},
			rttStats,
			connStats,
			initialMaxDatagramSize,
			false,
			qlogger,
		)
	case CongestionControlBBRv3:
		return NewBBRv3Sender(
			DefaultClock{},
			rttStats,
			connStats,
			initialMaxDatagramSize,
			qlogger,
		)
	default:
		panic("unknown congestion control: " + name)
	}
}
