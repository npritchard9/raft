const std = @import("std");
const Node = @import("node.zig").Node;
const LogEntry = @import("node.zig").LogEntry;

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

    pub fn tick(self: *Cluster) !void {
        const now = std.time.milliTimestamp();
        for (&self.nodes) |*node| {
            switch (node.role) {
                .Leader => {
                    for (&self.nodes) |*peer| {
                        if (peer.id == node.id) continue;
                        _ = try peer.appendEntries(node.id, node.currentTerm, node.commitIndex, &[_]LogEntry{}, now);
                    }
                },
                .Follower => {
                    if (node.checkElectionTimeout(now)) {
                        try self.startElection(node, now);
                    }
                },
                else => {},
            }
        }
        if (std.crypto.random.intRangeAtMost(u64, 0, 9) == 0) {
            const cmd = "set x=5";
            try self.simulateClientRequest(cmd);
        }
    }

    pub fn startElection(self: *Cluster, candidate: *Node, now: i64) !void {
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
                _ = try peer.appendEntries(candidate.id, candidate.currentTerm, candidate.commitIndex, &[_]LogEntry{}, now);
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

    pub fn clientRequest(self: *Cluster, nodeId: u32, command: []const u8) !void {
        var leader: ?*Node = null;
        var target: ?*Node = null;
        const now = std.time.milliTimestamp();

        for (&self.nodes) |*n| {
            if (n.id == nodeId) {
                target = n;
            }
            if (n.role == .Leader) {
                leader = n;
            }
        }
        if (leader == null) {
            std.debug.print("No leader currently selected. Client must retry\n", .{});
            return;
        }
        if (target == null) {
            return;
        }
        if (target.?.role != .Leader) {
            std.debug.print("Node {d} redirected client to Leader {d}\n", .{ target.?.id, leader.?.id });
        }
        const entry = LogEntry{
            .index = @intCast(leader.?.log.items.len),
            .term = leader.?.currentTerm,
            .command = command,
        };
        try leader.?.handleClientCommand(entry);
        try self.replicateLog(leader.?, entry, now);
    }

    pub fn replicateLog(self: *Cluster, leader: *Node, entry: LogEntry, now: i64) !void {
        var success_count: usize = 1;
        for (&self.nodes) |*peer| {
            if (peer.id == leader.id) continue;
            if (try peer.appendEntries(leader.id, leader.currentTerm, leader.commitIndex, &[1]LogEntry{entry}, now)) {
                success_count += 1;
            }
        }

        if (success_count > self.nodes.len / 2) {
            leader.commitIndex += 1;
            leader.applyCommitted();
        }
    }

    pub fn simulateClientRequest(self: *Cluster, command: []const u8) !void {
        const nodeIndex = std.crypto.random.intRangeAtMost(u64, 0, self.nodes.len - 1);
        const target = &self.nodes[nodeIndex];
        try self.clientRequest(target.id, command);
    }
};
