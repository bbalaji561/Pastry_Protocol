# Failure Model:
Implemented failure model by randomly selecting the number of nodes to fail from the built network which is given as one of the inputs from the user.
### Input:
1. numNodes - Number of peers to be created
2. numRequests - Number of requests each peer has to make
3. numPeersFail - Number of peers to fail in the network
### Command to run:
dotnet fsi --langversion:preview project3-bonus.fsx <numNodes> <numRequests> <numPeersFail>
### Objective:
We wanted to see how the application recovers from the failures and scales itself in handling the requests and how the average number of hops ranges. We also analyzed how the state tables are updated and requests are routed with multiple approaches which are slightly differing from each other. We finally decided to implement the recovery model as explained below:
### How will it work in case of failure:
When the request is about to be made to a failed node, we replace it with the alive node by gathering information from it’s neighbors as below and route the request with the message and keyId to it.

If the dead node is found in the leaf set, we select the alive node with largest prefix match in its neighbor id space and ask it for it’s leaf table. Then we update the current node’s leaf table with the returned alive values. Later, appropriately we select a possible node from the updated state tables and route the request to the same.

If the dead node is found in the routing table say Rd​​l,​ then we repair it by asking the other nodes in the same row, l for it’s Rd​ ​l value. We update the present node’s respective cell with the returned value and route the request to it.

If there are no such valid values found in leaf set and routing table, then we select the one from alive nodes present in the current state tables that is closest to the keyId and route the request to it.
     
Inorder to achieve this, we took the number of nodes to be failed from input and and selected them randomly from the generated set of nodes. Later initialized all the state tables as it would be in a non-failure model. Also, failure nodes will not generate routing requests.

Once the routing requests starts, the failure scenarios are properly handled as described above.
### Observations:
50 Requests were made by each peer in the network:

% - Percentage of nodes failed in the network
![Bonus_Table_1](https://github.com/bbalaji561/Pastry_Protocol/blob/main/Images/Bonus_Table_1.png)
![Bonus_Graph_1](https://github.com/bbalaji561/Pastry_Protocol/blob/main/Images/Bonus_Graph_1.png)


### Analysis:
Since whenever a failed node is encountered we update the failed nodes with the alive nodes. Then the application is able to route the requests to its destination as expected with almost the same average hops count as the non-failure model when the number of requests processed are higher (Though there is not much difference observed with our approach).

When we remove less number of nodes, the total requests need to be made and we get more average hops count. But when the number of nodes removed are more, since the failed will not make any requests, the total number of requests made are less and average hops count is also less as the requests will reach any of its closest alive neighbors.
