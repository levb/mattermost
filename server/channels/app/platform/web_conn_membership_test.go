// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package platform

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mattermost/mattermost/server/public/model"
)

// membershipTestConn builds an authenticated WebConn whose membership cache is
// primed with the given channels, so ShouldSendEvent takes the membership
// lookup path without hitting the store.
func membershipTestConn(tb testing.TB, th *TestHelper, members map[string]string) *WebConn {
	tb.Helper()

	wc := &WebConn{
		Platform: th.Service,
		Suite:    th.Suite,
		UserId:   model.NewId(),
		send:     make(chan model.WebSocketMessage, 256),
	}
	wc.SetConnectionID(model.NewId())
	wc.SetSession(&model.Session{UserId: wc.UserId, Token: model.NewId()})
	wc.SetSessionExpiresAt(model.GetMillis() + int64(time.Hour/time.Millisecond))
	wc.Active.Store(true)
	wc.allChannelMembers = compactChannelMembers(members)
	// Far enough in the future that the 30 minute expiry cannot fire mid-run.
	wc.lastAllChannelMembersTime = model.GetMillis() + int64(time.Hour/time.Millisecond)
	return wc
}

func channelEvent(channelID string) *model.WebSocketEvent {
	return model.NewWebSocketEvent(model.WebsocketEventPosted, "", channelID, "", nil, "")
}

// TestWebConnMembershipLookupAllocs pins the allocation count of the membership
// lookup path so that changes to the cache representation cannot regress it.
// It needs Setup (and so the test database) only because shouldSendEvent reads
// the config through the PlatformService.
func TestWebConnMembershipLookupAllocs(t *testing.T) {
	th := Setup(t)
	th.Service.UpdateConfig(func(cfg *model.Config) {
		*cfg.ServiceSettings.EnableWebHubChannelIteration = false
	})

	memberCh := model.NewId()
	otherCh := model.NewId()
	wc := membershipTestConn(t, th, map[string]string{memberCh: model.ChannelUserRoleId})

	memberEvent := channelEvent(string([]byte(memberCh)))
	nonMemberEvent := channelEvent(otherCh)

	require.True(t, wc.ShouldSendEvent(memberEvent))
	require.False(t, wc.ShouldSendEvent(nonMemberEvent))

	// The hub path's pre-resolved handles. nonMemberHandle also pins otherCh
	// in the unique table for the whole test: the table is weak, so without a
	// live handle a GC mid-loop could evict the entry and the next Make would
	// clone it again.
	memberHandle := compactChannelID(memberEvent.GetBroadcast().ChannelId)
	nonMemberHandle := compactChannelID(nonMemberEvent.GetBroadcast().ChannelId)
	defer runtime.KeepAlive(nonMemberHandle)

	// Everything before the membership check (the MFA request context) costs
	// one allocation; the membership lookup itself must add none, on the member
	// path and on the non-member path where the event's channel ID is already
	// compacted by another connection or an earlier event.
	baseline := testing.AllocsPerRun(1000, func() {
		wc.ShouldSendEvent(memberEvent)
	})
	t.Logf("member path allocs/op: %v", baseline)
	nonMember := testing.AllocsPerRun(1000, func() {
		wc.ShouldSendEvent(nonMemberEvent)
	})
	t.Logf("non-member path allocs/op: %v", nonMember)

	// The hub path with the handle resolved once per broadcast.
	hubMember := testing.AllocsPerRun(1000, func() {
		wc.shouldSendEvent(memberEvent, memberHandle)
	})
	hubNonMember := testing.AllocsPerRun(1000, func() {
		wc.shouldSendEvent(nonMemberEvent, nonMemberHandle)
	})
	t.Logf("hub member/non-member path allocs/op: %v/%v", hubMember, hubNonMember)
	require.Equal(t, baseline, hubMember)
	require.Equal(t, baseline, hubNonMember)
	require.LessOrEqual(t, baseline, float64(1), "member path must cost at most the MFA context allocation")
	require.Equal(t, baseline, nonMember, "membership lookup must not allocate on either path")
}
