# Distributed Auction

# Instruction to running the program

## First start the 3 RMs
The RMs should be started in a seperate terminal each with following command from project root
`go run ./RM`

## Second start one or more clients
The clients should be started in seperate terminals each with following command from project root
`go run ./client`


### client commands
Client has three options when it comes to commands
`bid <amount>`  //where amount is what the client wishes to bid
`result`        //returns the state of the auction and whether it is over or ongoing
`quit`          //stops the terminal from running as a client (you can also force quit ctrl+c)

## Testing crash
We recommend starting all 3 RM but 1+f RM (where f is amount of crashes/fails) should sufice

To test that the RM's are passing information and updating eachother begin an auction with some bids

Crash: close terminal window or ctrl+c in the terminal that runs the RM leader (the one with the lowest id)
Run result to see the auction state remains unchanged