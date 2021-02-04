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
let failureNodes = fsi.CommandLineArgs.[3] |> int
let rand = Random()

let system = ActorSystem.Create("Pastry", configuration)
type BossMessage = 
    | Start of (int*int)
    | Received of (int)
    | SendRequests of (int)

type Information = 
    | Request of (string)
    | Init of (Map<string,IActorRef>*string*int*int*Map<string,IActorRef>*List<string>)
    | Arrival of (string*IActorRef)
    | SendPathStateTables of (string*IActorRef*Map<int, Map<string, IActorRef>>*Map<int,Map<int,list<string>>>*Map<int,Set<string>>*Map<int,Set<string>>*Set<string>)
    | SendStateTable of (IActorRef*bool)
    | ReceivePathStateTables of (Map<int, Map<string, IActorRef>>*Map<int,Map<int,list<string>>>*Map<int,Set<string>>*Map<int,Set<string>>*Set<string>)
    | ReceiveSelfStateTable of (Map<int,list<string>>*Set<string>*Set<string>*Map<string, IActorRef>*bool)
    | RouteRequest of (string*int)
    | FailRouteRequest of (string*int)
    | RequestTables of (string*IActorRef)
    | SendToAll of (string)
    | RequestLeafSet of (string*string*int)
    | ReceiveLeafSet of (Set<string>*Set<string>*string*string*int)
    | RequestRoutingTableVal of (int*int*string*Set<string>*Set<string>*string*int)
    | ReceiveRoutingTableVal of (int*int*Set<string>*string*int)

let printActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! msg = mailbox.Receive()
        printfn "%A" msg
        return! loop()
    }
    loop()
let printerRef = spawn system "Printer" printActor

let PastryActors (mailbox:Actor<_>) = 
    let mutable adsmap = Map.empty
    let mutable smallLeafSet = Set.empty
    let mutable largeLeafSet = Set.empty
    let mutable neighbourMap = Map.empty
    let mutable routingTable = Map.empty
    let mutable pastryFailureNodes = []
    let mutable myId = ""
    let mutable myRef = mailbox.Self
    let mutable idLength = 0
    let mutable bossRef = mailbox.Self
    let mutable requestCount = 0
    let mutable maxRequest = 0
    let mutable updateReqSet = Set.empty

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

    let nodeInFailList checkId = List.exists (fun elem -> elem = checkId) pastryFailureNodes

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

    let updateLeafSet lfset rset = 
        let mutable tempset = Set.empty
        let mutable ls = Set.empty
        let mutable rs = Set.empty
        for ln in smallLeafSet do
            if not (nodeInFailList ln) then
                ls <- Set.add ln ls
        for ln in largeLeafSet do
            if not (nodeInFailList ln) then
                rs <- Set.add ln rs

        for ln in lfset do
            let pmatch = prefixMatchOther myId ln
            if ln.Length > 1 && (ln <> myId) && not (nodeInFailList ln) && pmatch > 0 then
                tempset <- Set.add ln tempset

        for ln in rset do
            let pmatch = prefixMatchOther myId ln
            if ln.Length > 1 && (ln <> myId) && not (nodeInFailList ln) && pmatch > 0 then
                tempset <- Set.add ln tempset                

        if tempset.Count > 1 then
            for eid in tempset do
                if eid < myId then
                    if ls.Count < 2 then
                        ls <- Set.add eid ls
                    else
                        let smaller = ls.MinimumElement
                        if eid > smaller then
                            ls <- Set.remove smaller ls
                            ls <- Set.add eid ls
                else if eid > myId then
                    if rs.Count < 2 then
                        rs <- Set.add eid rs
                    else
                        let larger = rs.MaximumElement
                        if eid < larger then
                            rs <- Set.remove larger rs
                            rs <- Set.add eid rs                    

            smallLeafSet <- ls
            largeLeafSet <- rs 
              
    let findNearerNode sk =
        let sknum = sk |> int
        let mutable nextNode = "" 
        let mutable pMatch = 0

        let mutable tempset = Set.empty
        for ln in largeLeafSet do
            if not (nodeInFailList ln) && ln.Length > 1 then
                tempset <- Set.add ln tempset
        
        for ln in smallLeafSet do
            if not (nodeInFailList ln) && ln.Length > 1 then
                tempset <- Set.add ln tempset

        for node in tempset do
            if (node <> sk) then
                let prefMatch = prefixMatchOther node sk
                if (prefMatch > pMatch) then
                    nextNode <- node
                    pMatch <- prefMatch
                else if (prefMatch = pMatch) then
                    if nextNode.Length = 0 then 
                        nextNode <- node
                        pMatch <- prefMatch
                    else
                        let nodenum = node |> int
                        let nextNodenum = nextNode |> int
                        let diff1 = abs (sknum-nodenum)
                        let diff2 = abs (sknum-nextNodenum)
                        if diff1 < diff2 then
                            nextNode <- node
        nextNode
    
    let findCorrectLeafNode searchk hcnt = 
        let mutable nextNode = ""
        nextNode <- findNearerNode searchk
        if nextNode.Length = 0 then
            "RT"
        else
            adsmap.[nextNode] <! RequestLeafSet(myId,searchk,hcnt)
            "pass"

    let findCorrectRTNode curFail row col hkey hcont = 
        let mutable routeset = Set.empty
        let checkList = routingTable.[row]
        for node in checkList do
            if (node.Length > 1) && node <> curFail && not (nodeInFailList node) then
                routeset <- Set.add node routeset
        
        if routeset.Count > 0 then
            let top = routeset.MaximumElement
            routeset <- Set.remove top routeset
            adsmap.[top] <! RequestRoutingTableVal(row, col, myId, routeset, Set.empty, hkey, hcont)
            "pass"
        else
            "fail"

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

    let checkAndRouteFail key hc = 
        let mutable ret = myId
        let numkey = key |> int
        let mutable left = false
        let mutable right = false
        let mutable rtbool = true
        if (smallLeafSet.Count = 0) || (smallLeafSet.MaximumElement < key) then
            left <- true
        if (largeLeafSet.Count = 0) || (largeLeafSet.MinimumElement > key) then
            right <- true  
        if left && right && (smallLeafSet.Count > 0 || largeLeafSet.Count > 0) then
            let mutable maxPrefix = prefixMatch key
            let (temp1, temp2) = checkCloser ret smallLeafSet numkey key maxPrefix
            ret <- temp1
            maxPrefix <- temp2
            let (temp3, temp4) = checkCloser ret largeLeafSet numkey key maxPrefix
            ret <- temp3
            maxPrefix <- temp4   

            if (nodeInFailList ret) then
                // printfn "failed-in-LS"
                let re = findCorrectLeafNode key hc
                if re = "pass" then
                    rtbool <- false
                    ret <- "pass"

        if rtbool then
            ret <- myId
            let pre = prefixMatch key
            let digit = (int key.[pre]) - (int '0')
            
            let mutable boolelse = false
            if (routingTable.ContainsKey (pre)) then
                let matchedRow = routingTable.[pre]
                if digit < 4 && matchedRow.[digit] <> "X" && matchedRow.[digit].Length > 1 then
                    ret <- matchedRow.[digit]
                    if (nodeInFailList ret) then 
                        // printfn "failed-in-RT"
                        let mutable newRTNode = findCorrectRTNode ret pre digit key hc
                        if (newRTNode = "fail") then
                            boolelse <- true
                        else if (newRTNode = "pass") then
                            ret <- "pass"
                else 
                    boolelse <- true
            else
                boolelse <- true

            if boolelse then   
                ret <- myId
                let checkNearer cList cNow toWhom numtoWhom = 
                    let mutable rt = cNow
                    let mutable cont = true
                    for (leaf:string) in cList do
                        if cont && leaf.Length > 1 && not (nodeInFailList leaf) then
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
        | Init(neigh, me, rows, reqs, admp, failNodesList) ->
            myId <- me
            idLength <- rows
            neighbourMap <- neigh
            bossRef <- mailbox.Sender()
            maxRequest <- reqs
            adsmap <- admp
            pastryFailureNodes <- failNodesList
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

            if not (nodeInFailList myId) then
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
            while nodeid.Length = 0 || nodeid = myId || (nodeInFailList nodeid) do
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
                let next = checkAndRouteFail mkey hopcount
                if next.Length > 1 && (next = "pass") then
                    ignore()
                else if next.Length > 1 && (next <> myId) && (adsmap.ContainsKey next) then
                    adsmap.[next] <! RouteRequest(mkey, hopcount+1)
                else
                    bossRef <! Received(hopcount)
            else
                bossRef <! Received(hopcount)
        | FailRouteRequest(mkey, hopcount) ->
            // printerRef <! sprintf "InsideRouteReq-id-%s-routing-%A" myId routingTable
            if (mkey <> myId) && (mkey.Length > 1) then
                let next = checkAndRoute mkey
                // printfn "7-id-%s-next-%s" myId next
                if next.Length > 1 && (next <> myId) && (adsmap.ContainsKey next) && not (nodeInFailList next) then
                    // printerRef <! sprintf "id-%s-routenext-%s-key-%s" myId next mkey
                    // printfn "id-%s-routenext-%s-key-%s" myId next mkey
                    adsmap.[next] <! RouteRequest(mkey, hopcount+1)
                else
                    bossRef <! Received(hopcount)
            else
                bossRef <! Received(hopcount)                
        | RequestLeafSet(sendId, rkey, hcount) ->
            // printfn "1a-LeafSmallSetinSender-%A" smallLeafSet
            adsmap.[sendId] <! ReceiveLeafSet(smallLeafSet, largeLeafSet, myId, rkey, hcount)
        | ReceiveLeafSet(smallSet, largeSet, id, ky, cnt) ->
            updateReqSet <- Set.remove id updateReqSet
            updateLeafSet smallSet largeSet
            mailbox.Self <! FailRouteRequest(ky, cnt)
            // printfn "1b-LeafSmallSetinReceiver-%A" failSmallLeafSet
        | RequestRoutingTableVal(r, c, sendId, path:Set<string>, fill, rk, hc) ->
            let reqRow = routingTable.[r]
            let answer = reqRow.[c]
            let mutable fl = fill
            if answer.Length > 1 && not (nodeInFailList answer) then
                fl <- Set.add answer fl
            if path.Count > 0 then
                let mutable ph:Set<string> = path
                let top = ph.MaximumElement
                ph <- Set.remove top ph
                adsmap.[top] <! RequestRoutingTableVal(r, c, sendId, ph, fl, rk, hc)
            else
                adsmap.[sendId] <! ReceiveRoutingTableVal(r, c, fl, rk, hc)
        | ReceiveRoutingTableVal(row, col, answer, rky, hcnt) ->
            for nd in answer do
                if nd.Length > 1 && nd <> myId && not (nodeInFailList nd) then
                    checkInsertRoutingTable nd
            
            mailbox.Self <! FailRouteRequest(rky, hcnt)
        | _ -> 
            ignore()

        return! loop()
    }
    loop()

let BossActor (mailbox:Actor<_>) = 
    let mutable requestsDone = 0
    let mutable hopCounts = 0
    let mutable totalRequests = 0
    let mutable ovr = 0
    let mutable itd = 0
    let mutable initint = 0
    let mutable nodeIdsList = []
    let mutable failureNodesList = []
    let mutable nodeIdsMap = Map.empty
    let mutable rcount = 1
    let rec loop () = actor {
        let! message = mailbox.Receive()
        match message with 
        | Start(nodes, requests) -> 
            totalRequests <- nodes * requests
            totalRequests <- totalRequests-(failureNodes*requests)
            // printfn "TotalRequests-%d" totalRequests
            let length = ceil (Math.Log(nodes |> float,4.0)) |> int
            // printfn "length %d" length
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

            // Building failureNodesList
            let hasNodeInFailList checkId list = List.exists (fun elem -> elem = checkId) list
            let mutable rem = 0
            let remRand = Random()
            while rem < failureNodes do
                let fNode = nodeIdsList.[remRand.Next(nodeIdsList.Length)]
                let fNodeAlreadyPresent = hasNodeInFailList fNode failureNodesList
                if not fNodeAlreadyPresent then
                    failureNodesList <- fNode :: failureNodesList
                    rem <- rem + 1
            printfn "Failure Nodes list: %A" failureNodesList
            
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
                nodeIdsMap.[nodeIdsList.[i]] <! Init(neigh, nodeIdsList.[i], length, requests, nodeIdsMap, failureNodesList)
            
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
