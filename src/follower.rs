use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, RequestVoteRequest, RequestVoteResponse,
};
use crate::{Raft, Request, Response, State, Transport};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub(crate) struct FollowerFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}

impl<'a, T> FollowerFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }
    pub async fn run(mut self) {
        tokio::select! {
            _ = sleep(Duration::from_millis(1000)) => self.raft.state = State::Candidate,
            Some(req) = self.raft.rx.recv() => match req {
                // Message::AppendEntries(req) => self.raft.handle_append_entries(req).await,
                Request::AppendEntries(req) => self.handle_append_entries(req).await,
                Request::RequestVote(req) => self.handle_request_vote(req).await,
            }
        }
    }

    async fn handle_append_entries(&mut self, mut req: AppendEntriesRequest) {
        let mut rep = AppendEntriesResponse::default();
        if self.raft.current_term > req.term {
            rep.term = self.raft.current_term;
            rep.success = false;
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            // return false
            return;
        } else if req.term > self.raft.current_term {
            self.raft.current_term = req.term;
        }
        rep.term = self.raft.current_term;

        let mut idx = self.raft.logs.len() - 1;
        let matched;
        let prev_log = req.prev_log.unwrap();
        loop {
            let log_id = self.raft.logs[idx].id.clone().unwrap();
            if log_id.term == prev_log.term && log_id.index == prev_log.index {
                matched = true;
                break;
            }
            idx -= 1;
        }
        if !matched {
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }

        // ????????????????????????????????????????????????????????????????????? ????????????????????????????????????????????????????????????
        // ?????????????????????
        idx = self.raft.logs.len() - 1;
        let conflict;
        loop {
            let log_id = self.raft.logs[idx].id.clone().unwrap();
            if log_id.index == prev_log.index {
                conflict = true;
                break;
            }
            idx -= 1;
        }
        if conflict {
            self.raft.logs.drain(idx..);
        }

        // ???????????????????????????
        self.raft.logs.append(req.entries.as_mut());

        // ???????????????????????????????????????????????????????????? ?????? ??????????????????????????????????????????????????????
        // (leaderCommit > commitIndex)????????? commitIndex ????????? leaderCommit
        if req.leader_committed_index > self.raft.committed_log_index {
            self.raft.committed_log_index = req.leader_committed_index;
        }

        rep.success = true;
        rep.term = self.raft.current_term;
        _ = self
            .raft
            .transport
            .send(req.leader_id, Response::AppendEntries(rep));
    }

    async fn handle_request_vote(&mut self, _req: RequestVoteRequest) {}
}
