use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, Message, RequestVoteRequest,
    RequestVoteResponse,
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
    break_loop: bool,
    vote_counter: usize,
}

impl<'a, T> CandidateFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self {
            raft,
            break_loop: false,
            vote_counter: 0,
        }
    }

    pub async fn req(&self, req: Request) {}

    // 向所有 Peers 送 request_vote 请求, 收到过半则转变为 Leader;
    // 收到新的 append_entries 则转变为 Follower;
    // 超时则开启新任期
    pub async fn run(mut self) {
        self.raft.current_term += 1;
        let election_timeout = 1000;
        while !self.break_loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(election_timeout)) => self.raft.state = State::Candidate,
                Some(req) = self.raft.in_rx.recv() => match req {
                    Message::AskForVoteReq(msg) => self.handle_request_vote_request(msg).await,
                    Message::AskForVoteRep(msg) => self.handle_request_vote_response(msg).await,
                    Message::AppendEntriesReq(msg) => self.handle_append_entries_request(msg).await,
                    Message::AppendEntriesRep(msg) => self.handle_append_entries_response(msg).await,
                },
                else => self.gather_votes().await,
            }
        }
    }

    async fn handle_append_entries_request(&mut self, req: AppendEntriesRequest) {
        if req.term > self.raft.current_term {
            self.raft.state = State::Follower;
            self.raft.current_term = req.term;
            self.break_loop = true;
        }
    }
    async fn handle_append_entries_response(&self, _req: AppendEntriesResponse) {
        unreachable!();
    }
    async fn handle_request_vote_request(&mut self, req: RequestVoteRequest) {
        if req.term > self.raft.current_term {
            self.raft.state = State::Follower;
            self.break_loop = true;
        }
    }
    async fn handle_request_vote_response(&mut self, rep: RequestVoteResponse) {
        if rep.granted {
            self.vote_counter += 1;
            if self.vote_counter > self.raft.peers.len() / 2 {
                self.raft.state = State::Leader;
            }
        }
    }
    async fn gather_votes(&self) {
        for peer in &self.raft.peers {
            _ = self.raft.transport.send(
                *peer,
                Request::RequestVote(RequestVoteRequest {
                    term: self.raft.current_term,
                    candidate_id: self.raft.id,
                    last_log_id: Some(LogId {
                        term: self.raft.current_term - 1,
                        index: self.raft.new_log_index - 1,
                    }),
                }),
            );
        }
    }
}
