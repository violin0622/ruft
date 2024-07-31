use crate::{
    AppendEntries, AppendEntriesRequest, AppendEntriesResponse, LogId, Message, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::{Raft, Request, Response, State, Transport};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub(crate) struct LeaderFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}
impl<'a, T> LeaderFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }

    pub async fn run(self) {
        let mut interval = time::interval(time::Duration::from_millis(1000));
        // tokio::interval
        // handle requests
        tokio::select! {
            _ = interval.tick() => self.send_append_entries_as_heartbeat().await,

            Some(req) = self.raft.in_rx.recv() => match req {
                Message::AppendEntriesReq(msg) => self.handle_append_entries_request(msg).await,
                Message::AppendEntriesRep(msg) => self.handle_append_entries_response(msg).await,
                Message::AskForVoteReq(msg) => self.handle_request_vote_request(msg).await,
                Message::AskForVoteRep(msg) => self.handle_request_vote_response(msg).await,
            },

            // 作为首领，还必须负责处理客户端的提案， 提案也通过 channel 传递。
        };
    }

    // 如果一段时间时间内没有新的日志，则向所有随从发送空的 AppendEntries 作为心跳。
    // send 只是个发送动作，它不应该失败。如果真的出现了不可恢复的错误，那么就 panic。
    async fn send_append_entries_as_heartbeat(&self) {
        for peer in &self.raft.peers {
            self.raft
                .transport
                .send(
                    *peer,
                    Request::AppendEntries(AppendEntriesRequest {
                        term: self.raft.current_term,
                        leader_id: self.raft.id,
                        prev_log: Some(LogId {
                            term: self.raft.current_term,
                            index: self.raft.new_log_index - 1,
                        }),
                        entries: vec![],
                        leader_committed_index: 0,
                    }),
                )
                .expect("Send heartbeat");
        }
    }

    // 收到 append_entries 响应有几种情况：
    // 1. 收到了来自当前任期的响应，那么就更新随从的 next_index 和 match_index。
    // 2. 收到了来自上一任期的响应，那么就忽略。
    // 3. 收到了来自下一任期的响应，那么就转换为跟随者。
    // 4. 收到了来自当前任期的响应，但是随从的日志不匹配，那么就减小 next_index 重试。
    async fn handle_append_entries_response(&self, _rep: AppendEntriesResponse) {}
    async fn handle_request_vote_response(&self, _rep: RequestVoteResponse) {}
    async fn handle_append_entries_request(&self, _req: AppendEntriesRequest) {}
    async fn handle_request_vote_request(&self, _req: RequestVoteRequest) {}
}
