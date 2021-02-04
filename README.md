# Pastry_Protocol
Implemented a Pastry algorithm by handling network join and routing apis. When a pastry node is given a message and key, using the pastry algorithm it routes to the live node which is numerically closest to the key. 
- Reference: ​Pastry: Scalable, decentralized object location and routing for large-scale peer-to-peer systems by Antony Rowstron and Peter Druschel

### Setup:
* We chose, the value of b = 2 
* Entries in Leaf set: 2^b
  * Large Leaf Set = 2^b / 2
  * Small Leaf Set = 2^b / 2
* Entries in Routing Table:
  * Rows = log​ pow(2, b)​ ​<numNodes>
  * Columns = 2^b
* Entries in Neighbour Set: 2^b

### Largest Network:​ 250,000
Our b is 2, so each peer id will be a base 2b​ value which is in combination of [0,1,2,3] - 4 possible digits. Therefore, our algorithm could support and generate a network with a max of 49​ peers in them => 262,144 peers.

So supported number of nodes as input is: 10 < Input # of nodes <= 250,000

### Input:
1. numNodes - Number of peers to be created
2. numRequests - Number of requests each peer has to make

### Command to run:
dotnet fsi --langversion:preview project3.fsx \<numNodes> \<numRequests>

### What is working:
Created ​peers using a random generator and values are of sequence of digits with base 2b​ ​. ​Started to build the network by creating actors for each of the peers and initialized their respective state’s: leaf set - both large and small leaf sets, neighbour set and routing tables to empty. Since the neighbour states are assumed to be calculated based on proximity metric which we cannot do in our application, we assumed the neighbours as the peers received before and after the current node in the DHT ring.

The nodes are joined one after the other through the admin actor which acts as a boss. As the new peer joins, it initializes the leaf set and the routing table by sending first a message having it’s ID along with a message to the previously generated peer (considering it to be the closest as we can’t check by proximity). The new peer’s state tables are updated by receiving the state tables collected along the path till it reaches the keyId which is the closest available in the network to it as per the algorithm in the paper. There are 3 possibilities to choose the nextId of the peer to whom the message has to route to, either the one in the leaf set with maximum prefix match or from the routing table or to a peer (all available peers in the state tables) with the Id that is closest to the KeyId.

Once the new peer’s state tables are populated, it sends it’s tables to all the peers contained in its state tables so they could update the arrival of a new peer and it starts sending out the requests. The requests are raised from each node and the message is the hop count number. Once the destination is reached it sends the hop count number to the boss handler which finally calculates the average hop count performed by all the peers for the requests in the network.
The message in average takes O(log N) steps to reach the destination peer.

### Observations:
#### Same Number of Requests by each peer in the network:
![alt text](http://url/to/img.png)

![alt text](http://url/to/img.png)


#### Different Number of Requests by each peer in the network:
![alt text](http://url/to/img.png)

![alt text](http://url/to/img.png)

### Analysis:
1. Initially, the message routing took more hops than the average calculated as the tables were not well-populated. But, once the tables were populated with peer ids the message routing happened in short hop counts.
2. The average hops count increases as the number of peers and also when the number of requests increases. Took more time for the large number of peers to converge as more actors need to be spawned and populated with state tables.
3. The requests always converge and their respective value of the average hop count is always within (log N) for every request.
