package congestion

import (
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

// RateSample describes the delivery rate observed by an ACK event.
type RateSample struct {
	Time           monotime.Time
	PriorDelivered protocol.ByteCount
	TotalDelivered protocol.ByteCount
	Delivered      protocol.ByteCount
	Interval       time.Duration
	RTT            time.Duration
	PriorInFlight  protocol.ByteCount
	BytesInFlight  protocol.ByteCount
}

// RateSampleAware is implemented by congestion controllers that need ACK
// delivery-rate samples.
type RateSampleAware interface {
	OnPacketAckedWithRateSample(protocol.ByteCount, RateSample)
}
