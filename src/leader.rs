use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, RequestVoteRequest, RequestVoteResponse,
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

            Some(req) = self.raft.rx.recv() => match req {
                Request::AppendEntries(_req) => self.handle_append_entries_request(_req).await,
                Request::RequestVote(_req) => self.handle_request_vote_request(_req).await,
            },

            Some(rep) = self.raft.rep_rx.recv() => match rep {
                Response::AppendEntries(_rep) => self.handle_append_entries_response(_rep).await,
                Response::RequestVote(_rep) => self.handle_request_vote_response(_rep).await,
            },
            // 作为首领，还必须负责处理客户端的提案， 提案也通过 channel 传递。
        };
    }

    // 如果一段时间时间内
    async fn send_append_entries_as_heartbeat(&self) {
        for peer in &self.raft.peers {
            _ = self.raft.transport.send(
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
            );
        }
    }

    async fn handle_append_entries_response(&self, _rep: AppendEntriesResponse) {}
    async fn handle_request_vote_response(&self, _rep: RequestVoteResponse) {}
    async fn handle_append_entries_request(&self, _req: AppendEntriesRequest) {}
    async fn handle_request_vote_request(&self, _req: RequestVoteRequest) {}
}
