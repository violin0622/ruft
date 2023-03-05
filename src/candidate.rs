use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, RequestVoteRequest, RequestVoteResponse,
};
use crate::{Raft, Request, Response, State, Transport};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub(crate) struct CandidateFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}

impl<'a, T> CandidateFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }

    // 向所有 Peers 送 request_vote 请求, 收到过半则转变为 Leader;
    // 收到新的 append_entries 则转变为 Follower;
    // 超时则开启新任期
    pub async fn run(self) {
        self.raft.current_term += 1;
        let mut counter = 1;
        let election_timeout = 1000;
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(election_timeout)) => self.raft.state = State::Candidate,
                Some(req) = self.raft.rx.recv() => match req {
                    Request::AppendEntries(req) => self.handle_append_entries_request(req).await,
                    Request::RequestVote(req) => self.handle_request_vote_request(req).await,
                },
                Some(rep) = self.raft.rep_rx.recv() => match rep {
                    Response::AppendEntries(_rep) => { unreachable!() },
                    Response::RequestVote(rep) => {
                        if rep.granted {
                            counter += 1;
                            if counter > self.raft.peers.len() / 2 {
                                self.raft.state = State::Leader;
                                break;
                            }
                        }
                    },
                },
                else => {
                    for peer in &self.raft.peers{
                        _ = self.raft.transport.send(
                            *peer,
                            Request::RequestVote(
                                RequestVoteRequest{
                                    term: self.raft.current_term,
                                    candidate_id: self.raft.id,
                                    last_log_id: Some(LogId{
                                        term: self.raft.current_term-1,
                                        index: self.raft.new_log_index-1,
                                    }),
                                }
                            )
                        );
                    }
                }
            }
        }
    }

    async fn handle_append_entries_request(&self, _req: AppendEntriesRequest) {}
    async fn handle_request_vote_request(&self, _req: RequestVoteRequest) {}
    async fn gather_votes(&self) {}
}
