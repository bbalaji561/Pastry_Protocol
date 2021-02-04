#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
open System
open System.Threading
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

// Input from Command Line
let nodes = fsi.CommandLineArgs.[1] |> int
let requests = fsi.CommandLineArgs.[2] |> int
let rand = Random()

let system = ActorSystem.Create("Pastry", configuration)
type BossMessage = 
    | Start of (int*int)
    | Received of (int)
    | Over of (string)
    | Initiated of (string)
    | InitTrue of (string)
    | Tables of (string)
    | SendRequests of (int)

type Information = 
    | Request of (string)
    | Init of (Map<string,IActorRef>*string*int*int*Map<string,IActorRef>)
    | Arrival of (string*IActorRef)
    | SendPathStateTables of (string*IActorRef*Map<int, Map<string, IActorRef>>*Map<int,Map<int,list<string>>>*Map<int,Set<string>>*Map<int,Set<string>>*Set<string>)
    | SendStateTable of (IActorRef*bool)
    | ReceivePathStateTables of (Map<int, Map<string, IActorRef>>*Map<int,Map<int,list<string>>>*Map<int,Set<string>>*Map<int,Set<string>>*Set<string>)
    | ReceiveSelfStateTable of (Map<int,list<string>>*Set<string>*Set<string>*Map<string, IActorRef>*bool)
    | RouteRequest of (string*int)
    | RequestTables of (string*IActorRef)
    | SendToAll of (string)
    | Route of (string)
    | Print of (int)

let PastryActors (mailbox:Actor<_>) = 
    let mutable adsmap = Map.empty
    let mutable smallLeafSet = Set.empty
    let mutable largeLeafSet = Set.empty
    let mutable neighbourMap = Map.empty
    let mutable routingTable = Map.empty
    let mutable myId = ""
    let mutable myRef = mailbox.Self
    let mutable idLength = 0
    let mutable bossRef = mailbox.Self
    let mutable requestCount = 0
    let mutable maxRequest = 0

    let prefixMatch (newId: string) = 
        let mutable ret = 0
        while (ret < idLength) && newId.[ret] = myId.[ret] do
            ret <- ret + 1
        ret

    let prefixMatchOther (newId: string) (otherId: string) = 
        let mutable ret = 0
        while (ret < idLength) && newId.[ret] = otherId.[ret] do
            ret <- ret + 1
        ret

    let checkInsertRoutingTable (checkId: string) = 
        if (checkId <> myId) then
            let matched = prefixMatch checkId
            let mutable matchedRow:list<string> = routingTable.[matched]
            let digit = (int checkId.[matched]) - (int '0')
            if matchedRow.[digit] <> "X" then
                let mutable insert = false
                if matchedRow.[digit].Length = 0 then 
                    insert <- true
                else
                    let myNum = myId |> int
                    let checkNum = checkId |> int
                    let entryNum = matchedRow.[digit] |> int
                    if (abs (checkNum-myNum)) < (abs (entryNum-myNum)) then 
                        insert <- true

                if insert then
                    matchedRow <- matchedRow |> List.mapi (fun i v -> if i = digit then checkId else v)
                    routingTable <- Map.remove matched routingTable
                    routingTable <- Map.add matched matchedRow routingTable

    let checkInsertSmallerLeafSet previd = 
        if smallLeafSet.Count < 2 then
            smallLeafSet <- Set.add previd smallLeafSet
        else
            let smaller = smallLeafSet.MinimumElement
            if previd > smaller then
                smallLeafSet <- Set.remove smaller smallLeafSet
                smallLeafSet <- Set.add previd smallLeafSet
                checkInsertRoutingTable smaller

    let checkInsertLargerLeftSet previd = 
        if largeLeafSet.Count < 2 then
            largeLeafSet <- Set.add previd largeLeafSet
        else
            let larger = largeLeafSet.MaximumElement
            if previd < larger then
                largeLeafSet <- Set.remove larger largeLeafSet
                largeLeafSet <- Set.add previd largeLeafSet
                checkInsertRoutingTable larger

    let routeToLeafSet (me:string) (incoming:string) = 
        if me.Length > 1 && incoming.Length > 1 then
            checkInsertRoutingTable incoming
            let pmatch = prefixMatchOther me incoming
            if pmatch > 0 then
                if incoming < me then
                    checkInsertSmallerLeafSet incoming
                else if incoming > me then
                    checkInsertLargerLeftSet incoming 
                
    let checkCloser closertillNow checkList nkey k mprefix = 
        let mutable ret1 = closertillNow
        let mutable mp = mprefix
        let mutable cont = true
        for leaf:string in checkList do
            if cont && leaf.Length > 1 then
                if leaf = k then
                    cont <- false
                    ret1 <- leaf
                    mp <- leaf.Length
                else
                    let pre = prefixMatchOther leaf k
                    if pre > mp then
                        mp <- pre
                        ret1 <- leaf
                    else if pre = mp then
                        let leafnum = leaf |> int
                        let retnum = ret1 |> int
                        let diff1 = abs (nkey-leafnum)
                        let diff2 = abs (nkey-retnum)
                        if diff1 < diff2 then
                            ret1 <- leaf
        (ret1, mp)

    let checkAndRoute key = 
        let mutable ret = myId
        let numkey = key |> int
        let mutable left = false
        let mutable right = false
        if (smallLeafSet.Count = 0) || (smallLeafSet.MaximumElement < key) then
            left <- true
        if (largeLeafSet.Count = 0) || (largeLeafSet.MinimumElement > key) then
            right <- true  
        if left && right then
            let mutable maxPrefix = prefixMatch key
            let (temp1, temp2) = checkCloser ret smallLeafSet numkey key maxPrefix
            ret <- temp1
            maxPrefix <- temp2
            let (temp3, temp4) = checkCloser ret largeLeafSet numkey key maxPrefix
            ret <- temp3
            maxPrefix <- temp4             
        else
            let pre = prefixMatch key
            let digit = (int key.[pre]) - (int '0')
            
            let mutable boolelse = false
            if (routingTable.ContainsKey (pre)) then
                let matchedRow = routingTable.[pre]
                if digit < 4 && matchedRow.[digit] <> "X" && matchedRow.[digit].Length > 1 then
                    ret <- matchedRow.[digit]
                else 
                    boolelse <- true
            else
                boolelse <- true

            if boolelse then   
                let checkNearer cList cNow toWhom numtoWhom = 
                    let mutable rt = cNow
                    let mutable cont = true
                    for (leaf:string) in cList do
                        if cont && leaf.Length > 1 then
                            if leaf = toWhom then
                                rt <- leaf
                                cont <- false
                            else
                                let leafpre = prefixMatchOther leaf toWhom
                                if leafpre = pre then
                                    let leafnum = leaf |> int
                                    let retnum = rt |> int
                                    let diff1 = abs (numtoWhom-leafnum)
                                    let diff2 = abs (numtoWhom-retnum)
                                    if diff1 < diff2 then
                                        rt <- leaf
                    rt

                ret <- checkNearer smallLeafSet ret key numkey
                ret <- checkNearer largeLeafSet ret key numkey

                for p in routingTable do
                    ret <- checkNearer p.Value ret key numkey
        ret 

    let rec loop () = actor {
        let! msg = mailbox.Receive()
        let mutable message : Information = msg
        match message with
        | Init(neigh, me, rows, reqs, admp) ->
            myId <- me
            idLength <- rows
            neighbourMap <- neigh
            bossRef <- mailbox.Sender()
            maxRequest <- reqs
            adsmap <- admp
            // Initialize empty routing table
            for r in [0 .. rows-1] do
                let mutable row = [""; ""; ""; ""]
                for c in [0 .. 3] do
                    let val1 = (int myId.[r]) - (int '0')
                    if val1 = c then
                        row <- row |> List.mapi (fun i v -> if i = c then "X" else v)
                routingTable <- Map.add r row routingTable
        | RequestTables(previd, prevref) ->
            let mutable neighState = Map.empty
            let mutable routeState = Map.empty
            let mutable smallLeafState = Map.empty
            let mutable largeLeafState = Map.empty
            let mutable sendersState = Set.empty
            routeToLeafSet myId previd
            prevref <! SendPathStateTables(myId, myRef, neighState, routeState, smallLeafState, largeLeafState, sendersState)
        | SendPathStateTables(sNode, sRef, nState, rtState, slState, lrState, adState) ->
            let next = checkAndRoute sNode
            routeToLeafSet myId sNode
            let ind = nState.Count
            let mutable _neighState = nState
            _neighState <- Map.add ind neighbourMap _neighState
            let mutable _routeState = rtState
            _routeState <- Map.add ind routingTable _routeState
            let mutable _smallLeafState = slState
            _smallLeafState <- Map.add ind smallLeafSet _smallLeafState
            let mutable _largeLeafState = lrState
            _largeLeafState <- Map.add ind largeLeafSet _largeLeafState
            let mutable _addressState = adState
            _addressState <- Set.add myId _addressState
            
            if next.Length > 1 && (next <> myId) && (next <> sNode) && (adsmap.ContainsKey next) then
                adsmap.[next] <! SendPathStateTables(sNode, sRef, _neighState, _routeState, _smallLeafState, _largeLeafState, _addressState)
            else
                sRef <! ReceivePathStateTables(_neighState, _routeState, _smallLeafState, _largeLeafState, _addressState)
            
        | ReceivePathStateTables(ns, rs, sls, lrs, ads) ->
            let trows = ns.Count
            let mutable tosend = Set.empty
            for i in [0 .. trows-1] do
                for pair in ns.[i] do
                    if not (ads.Contains pair.Key) then
                        tosend <- Set.add pair.Key tosend
                    routeToLeafSet myId pair.Key

                for row in rs.[i] do
                    for entry in row.Value do
                        if not (ads.Contains entry) then
                            tosend <- Set.add entry tosend
                        routeToLeafSet myId entry

                for node in sls.[i] do
                    if not (ads.Contains node) then
                        tosend <- Set.add node tosend
                    routeToLeafSet myId node

                for node in lrs.[i] do
                    if not (ads.Contains node) then
                        tosend <- Set.add node tosend
                    routeToLeafSet myId node

            mailbox.Self <! SendToAll("send")
        | SendStateTable(aref, islast) ->    
            aref <! ReceiveSelfStateTable(routingTable, smallLeafSet, largeLeafSet, neighbourMap, islast)
        
        | SendToAll(_) ->
            for lf in largeLeafSet do
                if (lf <> myId) && (adsmap.ContainsKey lf) then
                    adsmap.[lf] <! ReceiveSelfStateTable(routingTable, smallLeafSet, largeLeafSet, neighbourMap, false)
            
            for lf in smallLeafSet do
                if (lf <> myId) && (adsmap.ContainsKey lf) then
                    adsmap.[lf] <! ReceiveSelfStateTable(routingTable, smallLeafSet, largeLeafSet, neighbourMap, false)

            for pair in routingTable do
                for nd in pair.Value do
                    if (nd <> myId) && (adsmap.ContainsKey nd) then
                        adsmap.[nd] <! ReceiveSelfStateTable(routingTable, smallLeafSet, largeLeafSet, neighbourMap, false)

            for p in neighbourMap do
                p.Value <! ReceiveSelfStateTable(routingTable, smallLeafSet, largeLeafSet, neighbourMap, false)

            mailbox.Self <! Request("start")
        | ReceiveSelfStateTable(rt, slset, llset, nmap, tostart) ->
            for routerow in rt do
                for nodeentry in routerow.Value do
                    routeToLeafSet myId nodeentry
                   
            for nds in slset do
                routeToLeafSet myId nds

            for nde in llset do
                routeToLeafSet myId nde

            if tostart then
                mailbox.Self <! SendToAll("send")
        | Request(_) ->
            let digitsList = ["0"; "1"; "2"; "3"]
            let mutable nodeid = ""
            let reqrand = Random()
            while nodeid.Length = 0 || nodeid = myId do
                let mutable i = 0
                nodeid <- ""
                while i < idLength do
                    let nextDigit = digitsList.[reqrand.Next(digitsList.Length)]
                    nodeid <- nodeid + nextDigit
                    i <- i+1
             
            let nxt = checkAndRoute nodeid
            requestCount <- requestCount+1
            if requestCount < maxRequest then
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, Request("start"))

            if nxt.Length > 1 && (nxt <> myId) && (adsmap.ContainsKey nxt) then
                adsmap.[nxt] <! RouteRequest(nodeid, 1)
            else
                bossRef <! Received(0)
        | RouteRequest(mkey, hopcount) ->
            if (mkey <> myId) && (mkey.Length > 1) then
                let next = checkAndRoute mkey
                if next.Length > 1 && (next <> myId) && (adsmap.ContainsKey next) then
                    adsmap.[next] <! RouteRequest(mkey, hopcount+1)
                else
                    bossRef <! Received(hopcount)
            else
                bossRef <! Received(hopcount)
        | _ -> 
            ignore()

        return! loop()
    }
    loop()

let BossActor (mailbox:Actor<_>) = 
    let mutable requestsDone = 0
    let mutable hopCounts = 0
    let mutable totalRequests = 0
    let mutable nodeIdsList = []
    let mutable nodeIdsMap = Map.empty
    let mutable rcount = 1
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Start(nodes, requests) -> 
            totalRequests <- nodes * requests
            let length = ceil (Math.Log(nodes |> float,4.0)) |> int
            //printfn "length %d" length
            let digitsList = ["0"; "1"; "2"; "3"]
            let mutable num = 0
            let mutable nodeIdsSet = Set.empty
            while num < nodes do
                let mutable i = 0
                let mutable nodeid = ""
                while i < length do
                    let nextDigit = digitsList.[rand.Next(digitsList.Length)]
                    nodeid <- nodeid + nextDigit
                    i <- i+1
                if not (Set.contains nodeid nodeIdsSet) then
                    nodeIdsSet <- (Set.add nodeid nodeIdsSet)
                    nodeIdsList <- nodeid :: nodeIdsList
                    num <- num+1

            // printfn "firstnode-%s-length-%d" nodeIdsList.[0] nodeIdsList.Length
            for id in nodeIdsList do
                let ref = spawn system (sprintf "Actor_%s" id) PastryActors
                nodeIdsMap <- Map.add id ref nodeIdsMap

            // printfn "actors-spawned"
            for i in [0 .. nodeIdsList.Length-1] do
                let mutable neigh = Map.empty
                let mutable listid = nodeIdsList.[(nodes+i-2)%nodes]
                neigh <- Map.add listid nodeIdsMap.[listid] neigh
                listid <- nodeIdsList.[(nodes+i-1)%nodes]
                neigh <- Map.add listid nodeIdsMap.[listid] neigh
                listid <- nodeIdsList.[(nodes+i+1)%nodes]
                neigh <- Map.add listid nodeIdsMap.[listid] neigh
                listid <- nodeIdsList.[(nodes+i+2)%nodes]
                neigh <- Map.add listid nodeIdsMap.[listid] neigh
                // printfn "ref-for-%s--%A" nodeIdsList.[i] nodeIdsMap.[nodeIdsList.[i]]
                nodeIdsMap.[nodeIdsList.[i]] <! Init(neigh, nodeIdsList.[i], length, requests, nodeIdsMap)
            
            // printfn "actors-initiated"
            mailbox.Self <! SendRequests(1)
        | SendRequests(ni) ->
            if ni = 0 then
                nodeIdsMap.[nodeIdsList.[0]] <! SendToAll("send")
            else
                nodeIdsMap.[nodeIdsList.[ni]] <! RequestTables(nodeIdsList.[ni-1], nodeIdsMap.[nodeIdsList.[ni-1]])
            if (ni > 0) && (ni < nodeIdsList.Length-1) then
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(5.0), mailbox.Self, SendRequests(ni+1))
            else if (ni > 0) && (ni = nodeIdsList.Length-1) then
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(5.0), mailbox.Self, SendRequests(0))
        | Received(hops) -> 
            requestsDone <- (requestsDone+1)
            hopCounts <- (hopCounts+hops)
            if (requestsDone % 1000) = 0 then
                printfn "hops-count-%d-requestsdone-%d" hopCounts requestsDone
            if requestsDone = totalRequests then
                printfn "Total Hops: %d, totalRequests: %d Average Hop Count: %f" hopCounts totalRequests ((hopCounts |> float) / (totalRequests |> float))
                mailbox.Context.Stop(mailbox.Self)
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()


        return! loop()
    }
    loop()

// Start of the algorithm - spawn Boss, the delgator
let boss = spawn system "boss" BossActor
boss <! Start(nodes, requests)
system.WhenTerminated.Wait()
