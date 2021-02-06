# Replica Groups

It ensures reliability and availability when no more than a threshold of f replicas are faulty. It does this by using replica groups of size 2f + 1; this is the minimal number of replicas in an asynchronous network under the crash failure model.


```  
 ............................................................................     
 :     +-------------------------------------------------------------+      :
App    | Advance | Clock | Call | Propose | Membership | Tuple | ... |<--+  : 
 :     +-------------------------------------------------------------+   |  :
 ..................|.....................................................|...
          Producer |                                            Consumer |
 ..................|.....................................................|...
 :                 v                                                     |  :
 :   P +-->========================Bus==============================-----+  :
 :     |            |                                                       :
RSM    |            | C                                                     : 
 :     |   +--------v----------------+                                      :
 :     +---| Viewstamped Replication |                                      :
 :         +-------------------------+                                      :
 ............................................................................
```