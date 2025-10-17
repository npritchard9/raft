const std = @import("std");
const Node = @import("node.zig").Node;

pub const Cluster = struct {
    nodes: [5]Node,

    pub fn new() !Cluster {
        return Cluster{ .nodes = [5]Node{
            try Node.new(1),
            try Node.new(2),
            try Node.new(3),
            try Node.new(4),
            try Node.new(5),
        } };
    }

    pub fn tick(self: *Cluster) void {
        const now = std.time.milliTimestamp();
        for (&self.nodes) |*node| {
            switch (node.role) {
                .Leader => {
                    for (&self.nodes) |*peer| {
                        if (peer.id == node.id) continue;
                        peer.heartbeat(node.id, node.currentTerm, now);
                    }
                },
                .Follower => {
                    if (node.checkElectionTimeout(now)) {
                        self.startElection(node, now);
                    }
                },
                else => {},
            }
        }
    }

    pub fn startElection(self: *Cluster, candidate: *Node, now: i64) void {
        std.debug.assert(candidate.role != .Leader);
        candidate.role = .Candidate;
        candidate.currentTerm += 1;
        candidate.votedFor = candidate.id;
        candidate.resetElectionTimeout(now);

        var votes: usize = 1;

        for (&self.nodes) |*peer| {
            if (peer.id == candidate.id) continue;
            const lastIndex: u32 = if (candidate.log.items.len == 0) 0 else @intCast(candidate.log.items.len - 1);
            const lastTerm = if (candidate.log.items.len == 0) 0 else candidate.log.items[lastIndex].term;
            if (peer.requestVote(candidate.currentTerm, candidate.id, lastIndex, lastTerm, now)) {
                votes += 1;
            }
        }
        if (votes > self.nodes.len / 2) {
            candidate.becomeLeader();
            for (&self.nodes) |*peer| {
                if (peer.id == candidate.id) continue;
                peer.heartbeat(candidate.id, candidate.currentTerm, now);
            }
        } else {
            candidate.role = .Follower;
        }
    }
    pub fn printState(self: *Cluster) void {
        for (self.nodes) |node| {
            std.debug.print("Node {d}: role={s}, term={d}\n", .{ node.id, switch (node.role) {
                .Follower => "Follower",
                .Candidate => "Candidate",
                .Leader => "Leader",
            }, node.currentTerm });
        }
    }
};
