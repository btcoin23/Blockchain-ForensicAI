import Client, {
  CommitmentLevel,
  SubscribeRequestAccountsDataSlice,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterBlocks,
  SubscribeRequestFilterBlocksMeta,
  SubscribeRequestFilterEntry,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
} from "@triton-one/yellowstone-grpc";
import { SubscribeRequestPing } from "@triton-one/yellowstone-grpc/dist/grpc/geyser";
import { PublicKey, VersionedTransactionResponse } from "@solana/web3.js";
import { Idl } from "@project-serum/anchor";
import { SolanaParser } from "@shyft-to/solana-transaction-parser";
import { TransactionFormatter } from "./utils/transaction-formatter";
import pumpFunIdl from "./idls/pump_0.1.0.json";
import { SolanaEventParser } from "./utils/event-parser";
import { bnLayoutFormatter } from "./utils/bn-layout-formatter";
import { transactionOutput } from "./utils/transactionOutput";
import dotenv from 'dotenv';
dotenv.config();
interface SubscribeRequest {
  accounts: { [key: string]: SubscribeRequestFilterAccounts };
  slots: { [key: string]: SubscribeRequestFilterSlots };
  transactions: { [key: string]: SubscribeRequestFilterTransactions };
  transactionsStatus: { [key: string]: SubscribeRequestFilterTransactions };
  blocks: { [key: string]: SubscribeRequestFilterBlocks };
  blocksMeta: { [key: string]: SubscribeRequestFilterBlocksMeta };
  entry: { [key: string]: SubscribeRequestFilterEntry };
  commitment?: CommitmentLevel | undefined;
  accountsDataSlice: SubscribeRequestAccountsDataSlice[];
  ping?: SubscribeRequestPing | undefined;
}

const TXN_FORMATTER = new TransactionFormatter();
const PUMP_FUN_PROGRAM_ID = new PublicKey(
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
);

const PUMP_FUN_IX_PARSER = new SolanaParser([]);
PUMP_FUN_IX_PARSER.addParserFromIdl(
  PUMP_FUN_PROGRAM_ID.toBase58(),
  pumpFunIdl as Idl,
);
const PUMP_FUN_EVENT_PARSER = new SolanaEventParser([], console);
PUMP_FUN_EVENT_PARSER.addParserFromIdl(
  PUMP_FUN_PROGRAM_ID.toBase58(),
  pumpFunIdl as Idl,
);

async function handleStream(client: Client, args: SubscribeRequest) {
  // Subscribe for events
  const stream = await client.subscribe();

  // Create `error` / `end` handler
  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      console.log("ERROR", error);
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("close", () => {
      resolve();
    });
  });

  // Handle updates
  stream.on("data", (data) => {
    if (data?.transaction) {
      const txn = TXN_FORMATTER.formTransactionFromJson(
        data.transaction,
        Date.now(),
      );

      const parsedTxn = decodePumpFunTxn(txn);

      if ( !parsedTxn ) {
        let mintAddress = txn.transaction.message.staticAccountKeys.filter(item => item.toString().includes("pump"))[0];
        if (mintAddress != undefined) {
          const [bondingCurve] = PublicKey.findProgramAddressSync([Buffer.from("bonding-curve"), new PublicKey(mintAddress).toBytes()], PUMP_FUN_PROGRAM_ID);
          let index = -1;
          txn.transaction.message.staticAccountKeys.map((item, itemIndex) =>{
            if(item.toString() === bondingCurve.toString()) index = itemIndex;
          });
          if(index === -1 ) return ;
          const solbalance = (txn.meta.postBalances[index] - txn.meta.preBalances[index]) / 1000000000.0
          const preTokenBalances = txn.meta.preTokenBalances.filter((item) => item.mint === mintAddress.toString());
          const postTokenBalances = txn.meta.postTokenBalances.filter((item) => item.mint === mintAddress.toString());

          let tokenAmount = 0;
          preTokenBalances.map(item => {
            postTokenBalances.map(value => {
              if (value.accountIndex === item.accountIndex) {
                tokenAmount = item.uiTokenAmount.uiAmount - value.uiTokenAmount.uiAmount;
              }
            })
          })

          let isBuy = true;
          if (txn.meta.logMessages.join("").includes("Sell") || txn.meta.logMessages.join("").includes("sell")) {
            isBuy = false;
          }

          console.log(
            `
            FROM : NON-PUMPFUN PROGRAM
            TYPE : ${isBuy ? "BUY" : "SELL"}
            MINT : ${mintAddress}
            SIGNER : ${txn.transaction.message.staticAccountKeys[0]}
            TOKEN AMOUNT : ${Math.abs(tokenAmount)}
            SOL AMOUNT : ${Math.abs(solbalance)} SOL
            SIGNATURE : ${txn.transaction.signatures[0]}
            `
          )
        }
        return;
      }

      const tOutput = transactionOutput(parsedTxn)
      if (!tOutput) return; 
      if (!tOutput.solAmount) return; // in this case, create token transaction will ignore 
      console.log(
        `
        FROM : PUMP_FUN PROGRAM
        TYPE : ${tOutput.type}
        MINT : ${tOutput.mint}
        SIGNER : ${tOutput.user}
        TOKEN AMOUNT : ${tOutput.tokenAmount}
        SOL AMOUNT : ${tOutput.solAmount} SOL
        SIGNATURE : ${txn.transaction.signatures[0]}
        `
      )
    }
  });

  // Send subscribe request
  await new Promise<void>((resolve, reject) => {
    stream.write(args, (err: any) => {
      if (err === null || err === undefined) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    console.error(reason);
    throw reason;
  });

  await streamClosed;
}

async function subscribeCommand(client: Client, args: SubscribeRequest) {
  while (true) {
    try {
      await handleStream(client, args);
    } catch (error) {
      console.error("Stream error, restarting in 1 second...", error);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}


const client = new Client(
  process.env.GRPC,
  process.env.TOKEN,
  undefined,
);
const req: SubscribeRequest = {
  accounts: {},
  slots: {},
  transactions: {
    pumpFun: {
      vote: false,
      failed: false,
      signature: undefined,
      accountInclude: [
        PUMP_FUN_PROGRAM_ID.toBase58()

      ],
      accountExclude: [],
      accountRequired: [],
    },
  },
  transactionsStatus: {},
  entry: {},
  blocks: {},
  blocksMeta: {},
  accountsDataSlice: [],
  ping: undefined,
  commitment: CommitmentLevel.CONFIRMED,
};

subscribeCommand(client, req);

function decodePumpFunTxn(tx: VersionedTransactionResponse) {
  if (tx.meta?.err) return;

  const paredIxs = PUMP_FUN_IX_PARSER.parseTransactionData(
    tx.transaction.message,
    tx.meta.loadedAddresses,
  );

  const pumpFunIxs = paredIxs.filter((ix) =>
    ix.programId.equals(PUMP_FUN_PROGRAM_ID),
  );

  if (pumpFunIxs.length === 0) return;
  const events = PUMP_FUN_EVENT_PARSER.parseEvent(tx);
  const result = { instructions: pumpFunIxs, events };
  bnLayoutFormatter(result);
  return result;
}