## Node

A node is an abstraction of a cluster node. Each node manages multiple Raft groups. The node receives Raft messages from other nodes and processes them to drive the execution of the Raft algorithm. The node also functions as a heartbeat group to manage heartbeat information for all Raft instances on that node.

## Actor

NodeActor is an abstraction for the computation performed by a node. NodeActor runs in a separate asynchronous task, encapsulating state and data structures, and interacts with the outside world through messages.

After NodeActor runs, it returns a NodeHandle, which is used for communication with the NodeActor. Additionally, the NodeHandle is responsible for managing the lifecycle of the NodeActor.

- Handling requests from clients
- Handling messages from other Raft replicas
- Processing requests from administrators
- Managing the state of Raft groups
- Recording monitoring information 


```mermaid
classDiagram
   class NodeHandle {
        propose_tx: Sender~ProposeMessage~
        message_tx: Sender~NodeMessage~
        actor_handle: JoinHandle
        
        + async send_propose(msg: ProposeMessage)
        + send_propose_block (msg: ProposeMessage)
        + send_propose_non_block (msg: ProposeMessage)

        + async send_node_message(msg: NodeMessage)
        + send_node_message_block (msg: NodeMessage)
        + send_node_message_non_block (msg: NodeMessage)
        + async stop(&self)
        + async join(mut self) -> Result~()~
    }

    class NodeActor~T:Transport, MS: MultiRaftStorage~ {
      node_id: u64,
      transport: T
      storage: MS
      node_mgr: NodeManager
      group_mgr: GroupManager
      active_groups: HashSet<u64>
      replica_cache: ReplicaCache
      pending_res: ResponseQueue
      msg_rx: Receiver~NodeMessage~
      propose_rx: Receiver~ProposeMessage~
      apply_tx: Sender~ApplyMessage~
      reporter: Reporter

      spawn() -> Result~NodeHandle~
    }

    NodeActor <-- NodeHandle

```


The `NodeActor::spawn` method creates an actor and returns a `NodeHandle`. The `stop` method of the handle sends a stop signal to the actor, and you can use the `join` method to wait for the actor to finish.

## Actor Loop

TODO

## Heartbeat Merge


TODO

### A some corner case

TODO


```mermaid
classDiagram
    class Node{
        pub node_id: u64,
        pub groups: Vec~u64~,
    }

    class NodeManager{
        pub nodes: HashMap
    }

    NodeManager o-- Node

    class ReplicaDesc{
        pub node_id: u64
        pub group_id: u64
        pub replica_id: u64
    }

    class Proposal~Response: ProposeResponse~ {
        pub index: u64,
        pub term: u64,
        pub is_conf_change: bool,
        pub tx: Option~Sender~
    }

    class ProposalQueue~Response~{
        replica_id: u64
        queue: VecDeque~Proposal~
    }

    ProposalQueue *-- Proposal


    class ReadIndexProposal {
        pub uuid: Uuid
        pub read_index: Option~u64~
        pub context: Option~ReadIndexContext~
        pub tx: Option~Sender~
    }

    class ReadIndexQueue {
        ready_cnt: usize,
        handle_cnt: usize,
        queue: VecDeque~ReadIndexProposal~,
    }

    ReadIndexQueue *-- ReadIndexProposal

    class RaftGroup {
        node_id: u64
        group_id: u64
        replica_id: u64
        raft_ins: RawNode~S~
        track_nodes: Vec~u64~
        leader: ReplicaDesc
        commit_idx: u64
        commit_term: u64
        proposal_queue: ProposalQueue~Response~
        read_idx_queue: ReadIndexQueue
    }

    RaftGroup *-- ReplicaDesc
    RaftGroup *-- ProposalQueue
    RaftGroup *-- ReadIndexQueue
    

    

    class RaftGroupManager{
        pub groups: HashMap~u64->RaftGroup~
    }

    RaftGroupManager o-- RaftGroup
    

    class WriteRequest~WriteRequest, Response~{
        pub group_id: u64
        pub term: u64
        pub data: WriteRequest
        pub context: Option~Vec~
        pub tx: Option~Sender
    }

    class ReadIndexContext {
        pub uuid: [u8; 16],
        pub context: Option~Vec~,
    }

    class ReadIndexRequest {
        pub group_id: u64,
        pub context: ReadIndexContext,
        pub tx: ~Sender~
    }

    ReadIndexRequest *-- ReadIndexContext

    
    class  MembershipRequest~Response~ {
        pub group_id: u64,
        pub term: Option~u64~,
        pub context: Option~Vec~,
        pub data: MembershipChangeData,
        pub tx: oneshot::Sender,
    }


    class ProposeMessage~Request, Response~{
        <<enum>>
        Write(WriteRequest)
        Membership(MembershipRequest)
        ReadIndex(ReadIndexRequest)
    }

    

    ProposeMessage *-- WriteRequest
    ProposeMessage *-- MembershipRequest
    ProposeMessage *-- ReadIndexRequest

    class NodeMessage {
        Raft(MultiRaftMessage)
        Manage(ManageMessage)
        Campaign(CampaignMessage)
        Apply(ApplyCommitMessage)
        ApplyResult(ApplyResultMessage)
        Query(QueryMessage)
    }

    class NodeActor~T:Transport, MS: MultiRaftStorage~ {
      node_id: u64,
      transport: T
      storage: MS
      node_mgr: NodeManager
      group_mgr: GroupManager
      active_groups: HashSet<u64>
      replica_cache: ReplicaCache
      pending_res: ResponseQueue
      msg_rx: Receiver~NodeMessage~
      propose_rx: Receiver~ProposeMessage~
      apply_tx: Sender~ApplyMessage~
      reporter: Reporter
    }

    NodeActor o-- NodeManager
    NodeActor o-- RaftGroupManager
    NodeActor *-- ProposeMessage
    NodeActor *-- NodeMessage
```