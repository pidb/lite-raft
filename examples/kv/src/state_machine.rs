use std::future::Future;

use oceanraft::rsm_event;
use oceanraft::rsm_event::ApplyEvent;
use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::StorageExt;
use oceanraft::StateMachine;

use crate::server::KVServer;
use crate::server::{KVData, KVResponse};

impl StateMachine<KVData, KVResponse> for KVServer {
    type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0;
    fn apply<'life0>(
        &'life0 self,
        event: ApplyEvent<KVData, KVResponse>,
    ) -> Self::ApplyFuture<'life0> {
        async move {
            for apply in event.applys {
                let apply_index = apply.get_index();
                println!(
                    "group({}), replica({}) apply index = {}",
                    event.group_id, event.replica_id, apply_index
                );
                match apply {
                    rsm_event::Apply::NoOp(_) => {}
                    rsm_event::Apply::Normal(mut apply) => {
                        let res = KVResponse {
                            index: apply_index,
                            term: apply.term,
                        };
                        self.kv_storage.put(apply.data.key, apply.data.value);
                        // TODO: this call as method
                        apply
                            .tx
                            .map(|tx| tx.send(Ok((res, apply.context.take()))).unwrap());
                    }
                    rsm_event::Apply::Membership(apply) => {
                        apply.tx.map(|tx| {
                            tx.send(Ok((
                                KVResponse {
                                    index: apply.index,
                                    term: apply.term,
                                },
                                apply.ctx,
                            )))
                        });
                    }
                }
                // TODO: consider more easy api
                let gs = self
                    .log_storage
                    .group_storage(event.group_id, event.replica_id)
                    .await
                    .unwrap();
                gs.set_applied(apply_index).unwrap();
            }
        }
    }

    type OnLeaderElectionFuture<'life0> = impl Future<Output = ()> + 'life0;
    fn on_leader_election<'life0>(
        &'life0 self,
        event: oceanraft::rsm_event::LeaderElectionEvent,
    ) -> Self::OnLeaderElectionFuture<'life0> {
        async move { todo!() }
    }

    type OnGroupCreateFuture<'life0> = impl Future<Output = ()> + 'life0;
    fn on_group_create<'life0>(
        &'life0 self,
        event: oceanraft::rsm_event::GroupCreateEvent,
    ) -> Self::OnGroupCreateFuture<'life0> {
        async move { todo!() }
    }
}
