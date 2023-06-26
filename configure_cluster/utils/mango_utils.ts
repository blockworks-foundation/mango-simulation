import * as anchor from '@project-serum/anchor';
import { Market, OpenOrders } from "@project-serum/serum";
import * as mango_client_v3 from '@blockworks-foundation/mango-client';
import { Connection, LAMPORTS_PER_SOL } from '@solana/web3.js';
import * as splToken from '@solana/spl-token';
import {
    NATIVE_MINT,
    Mint,
    TOKEN_PROGRAM_ID,
    MintLayout,
} from "@solana/spl-token";

import {
    PublicKey,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    Signer,
} from '@solana/web3.js';
import { SerumUtils, } from "./serum_utils";
import { MintUtils, TokenData, } from './mint_utils';
import { BN } from 'bn.js';
import {
    GroupConfig,
    OracleConfig,
    PerpMarketConfig,
    SpotMarketConfig,
    TokenConfig,
    Cluster,
    PerpEventQueueHeaderLayout,
    PerpEventLayout,
    BookSideLayout,
    makeCreatePerpMarketInstruction,
    I80F48,
    PerpMarketLayout,
    makeAddPerpMarketInstruction,
    makeDepositInstruction,
    Config as MangoConfig,
    sleep
} from '@blockworks-foundation/mango-client';
import { token } from '@project-serum/anchor/dist/cjs/utils';
import { Config } from './config';

export interface PerpMarketData {
    publicKey: PublicKey,
    asks: PublicKey,
    bids: PublicKey,
    eventQ: PublicKey,
}

export interface MangoTokenData extends TokenData {
    rootBank: PublicKey,
    nodeBank: PublicKey,
    marketIndex: number,
    perpMarket: PerpMarketData,
}

export interface MangoCookie {
    mangoGroup: PublicKey,
    signerKey: PublicKey,
    mangoCache: PublicKey,
    usdcRootBank: PublicKey,
    usdcNodeBank: PublicKey,
    usdcVault: PublicKey,
    usdcMint: PublicKey,
    tokens: Array<[String, MangoTokenData]>,
    MSRM: PublicKey,
}

export interface MangoUser {
    kp: Keypair,
    mangoAddress: PublicKey,
}

export class MangoUtils {
    private conn: Connection;
    private serumUtils: SerumUtils;
    private mintUtils: MintUtils;
    private mangoClient: mango_client_v3.MangoClient;
    private authority: Keypair;
    private mangoProgramId: PublicKey;
    private dexProgramId: PublicKey;

    constructor(conn: Connection, authority: Keypair, mangoProgramId: PublicKey, dexProgramId: PublicKey, pythProgramId: PublicKey) {
        this.conn = conn;
        this.authority = authority;
        this.mangoProgramId = mangoProgramId;
        this.dexProgramId = dexProgramId;

        this.serumUtils = new SerumUtils(conn, authority, dexProgramId);
        this.mintUtils = new MintUtils(conn, authority, dexProgramId, pythProgramId);
        this.mangoClient = new mango_client_v3.MangoClient(conn, mangoProgramId);
    }

    async createAccountForMango(size: number): Promise<PublicKey> {
        const lamports = await this.conn.getMinimumBalanceForRentExemption(size);
        let address = Keypair.generate();

        const transaction = new Transaction().add(
            SystemProgram.createAccount({
                fromPubkey: this.authority.publicKey,
                newAccountPubkey: address.publicKey,
                lamports,
                space: size,
                programId: this.mangoProgramId,
            }))

        transaction.feePayer = this.authority.publicKey;
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;
        // Sign transaction, broadcast, and confirm
        await sendAndConfirmTransaction(
            this.conn,
            transaction,
            [this.authority, address],
            { commitment: 'confirmed' },
        );

        // airdrop all mongo accounts 1000 SOLs
        // const signature =  await this.conn.requestAirdrop(address.publicKey, LAMPORTS_PER_SOL * 1000);
        // const blockHash = await this.conn.getRecentBlockhash('confirmed');
        // const blockHeight = await this.conn.getBlockHeight('confirmed')
        // await this.conn.confirmTransaction({signature: signature, blockhash: blockHash.blockhash, lastValidBlockHeight: blockHeight});
        return address.publicKey;
    }

    public async createMangoCookie(tokensList: Array<String>): Promise<MangoCookie> {

        const size = mango_client_v3.MangoGroupLayout.span;
        let group_address = await this.createAccountForMango(size);
        let root_bank_address = await this.createAccountForMango(mango_client_v3.RootBankLayout.span);
        let node_bank_address = await this.createAccountForMango(mango_client_v3.NodeBankLayout.span);
        let mango_cache = await this.createAccountForMango(mango_client_v3.MangoCacheLayout.span);

        const { signerKey, signerNonce } = await mango_client_v3.createSignerKeyAndNonce(
            this.mangoProgramId,
            group_address,
        );

        let mangoCookie: MangoCookie = {
            mangoGroup: null,
            signerKey,
            mangoCache: null,
            usdcRootBank: null,
            usdcNodeBank: null,
            usdcVault: null,
            tokens: new Array<[String, MangoTokenData]>(),
            usdcMint: await this.mintUtils.createMint(6),
            MSRM: await this.mintUtils.createMint(6),
        };

        let usdc_vault = await this.mintUtils.createTokenAccount(mangoCookie.usdcMint, this.authority, signerKey);
        splToken.mintTo(this.conn, this.authority, mangoCookie.usdcMint, usdc_vault, this.authority, 1000000 * 1000000);
        mangoCookie.usdcVault = usdc_vault;

        let insurance_vault = await this.mintUtils.createTokenAccount(mangoCookie.usdcMint, this.authority, signerKey);
        splToken.mintTo(this.conn, this.authority, mangoCookie.usdcMint, insurance_vault, this.authority, 1000000 * 1000000);

        let fee_vault = await this.mintUtils.createTokenAccount(mangoCookie.usdcMint, this.authority, TOKEN_PROGRAM_ID);
        splToken.mintTo(this.conn, this.authority, mangoCookie.usdcMint, fee_vault, this.authority, 1000000 * 1000000);

        let msrm_vault = await this.mintUtils.createTokenAccount(mangoCookie.MSRM, this.authority, signerKey);
        mangoCookie.usdcRootBank = root_bank_address;
        mangoCookie.usdcNodeBank = node_bank_address;


        console.log('mango program id : ' + this.mangoProgramId)
        console.log('serum program id : ' + this.dexProgramId)

        let ix = mango_client_v3.makeInitMangoGroupInstruction(
            this.mangoProgramId,
            group_address,
            signerKey,
            this.authority.publicKey,
            mangoCookie.usdcMint,
            usdc_vault,
            node_bank_address,
            root_bank_address,
            insurance_vault,
            PublicKey.default,
            fee_vault,
            mango_cache,
            this.dexProgramId,
            new anchor.BN(signerNonce),
            new anchor.BN(10),
            mango_client_v3.I80F48.fromNumber(0.7),
            mango_client_v3.I80F48.fromNumber(0.06),
            mango_client_v3.I80F48.fromNumber(1.5),
        );

        let ixCacheRootBank = mango_client_v3.makeCacheRootBankInstruction(this.mangoProgramId,
            group_address,
            mango_cache,
            [root_bank_address]);

        let ixupdateRootBank = mango_client_v3.makeUpdateRootBankInstruction(this.mangoProgramId,
            group_address,
            mango_cache,
            root_bank_address,
            [node_bank_address]);

        await this.processInstruction(ix, [this.authority]);
        await this.processInstruction(ixCacheRootBank, [this.authority]);
        await this.processInstruction(ixupdateRootBank, [this.authority]);

        mangoCookie.mangoGroup = group_address;
        mangoCookie.mangoCache = mango_cache;
        console.log('Mango group created, creating tokens')
        // create mngo
        const mngoData = await this.createMangoToken(mangoCookie, tokensList[0], 0, 6, 100);
        let tokenData = await Promise.all(tokensList.filter((a,b)=> b > 0).map((tokenStr, tokenIndex) => this.createMangoToken(mangoCookie, tokenStr, tokenIndex+1, 6, 100)));
        tokenData.push(mngoData)
        await this.mangoClient.cachePrices(mangoCookie.mangoGroup, mangoCookie.mangoCache, tokenData.map(x=>x.priceOracle.publicKey), this.authority);
        //tokensList.map((tokenStr, tokenIndex) => this.createMangoToken(mangoCookie, tokenStr, tokenIndex, 6, 100));
        return mangoCookie;
    }

    public async createMangoToken(mangoCookie: MangoCookie, tokenName: String, tokenIndex: number, nbDecimals, startingPrice): Promise<MangoTokenData> {
        console.log('Creating token ' + tokenName + " at index " + tokenIndex)
        const tokenData = await this.mintUtils.createNewToken(mangoCookie.usdcMint, nbDecimals, startingPrice);
        let mangoTokenData: MangoTokenData = {
            market: tokenData.market,
            marketIndex: tokenIndex,
            mint: tokenData.mint,
            priceOracle: tokenData.priceOracle,
            nbDecimals: tokenData.nbDecimals,
            startingPrice: startingPrice,
            nodeBank: await this.createAccountForMango(mango_client_v3.NodeBankLayout.span),
            rootBank: await this.createAccountForMango(mango_client_v3.RootBankLayout.span),
            perpMarket: null,
        };
        // add oracle to mango
        let add_oracle_ix = mango_client_v3.makeAddOracleInstruction(
            this.mangoProgramId,
            mangoCookie.mangoGroup,
            mangoTokenData.priceOracle.publicKey,
            this.authority.publicKey,
        );
        // add oracle
        {
            const transaction = new Transaction();
            transaction.add(add_oracle_ix);
            transaction.feePayer = this.authority.publicKey;
            let hash = await this.conn.getRecentBlockhash();
            transaction.recentBlockhash = hash.blockhash;
            // Sign transaction, broadcast, and confirm
            await this.mangoClient.sendTransaction(
                transaction,
                this.authority,
                [],
                3600,
                'confirmed',
            );
        }
        await this.initSpotMarket(mangoCookie, mangoTokenData);

        const group = await this.mangoClient.getMangoGroup(mangoCookie.mangoGroup);
        mangoTokenData.marketIndex = group.getTokenIndex(mangoTokenData.mint)
        mangoTokenData.perpMarket = await this.initPerpMarket(mangoCookie, mangoTokenData);

        mangoCookie.tokens.push([tokenName, mangoTokenData]);
        return mangoTokenData;
    }

    public async initSpotMarket(mangoCookie: MangoCookie, token: MangoTokenData) {

        const vault = await this.mintUtils.createTokenAccount(token.mint, this.authority, mangoCookie.signerKey);
        // add spot market to mango
        let add_spot_ix = mango_client_v3.makeAddSpotMarketInstruction(
            this.mangoProgramId,
            mangoCookie.mangoGroup,
            token.priceOracle.publicKey,
            token.market.address,
            this.dexProgramId,
            token.mint,
            token.nodeBank,
            vault,
            token.rootBank,
            this.authority.publicKey,
            mango_client_v3.I80F48.fromNumber(10),
            mango_client_v3.I80F48.fromNumber(5),
            mango_client_v3.I80F48.fromNumber(0.05),
            mango_client_v3.I80F48.fromNumber(0.7),
            mango_client_v3.I80F48.fromNumber(0.06),
            mango_client_v3.I80F48.fromNumber(1.5),
        );

        let ixCacheRootBank = mango_client_v3.makeCacheRootBankInstruction(this.mangoProgramId,
            mangoCookie.mangoGroup,
            mangoCookie.mangoCache,
            [token.rootBank]);

        let ixupdateRootBank = mango_client_v3.makeUpdateRootBankInstruction(this.mangoProgramId,
            mangoCookie.mangoGroup,
            mangoCookie.mangoCache,
            token.rootBank,
            [token.nodeBank]);

        const transaction = new Transaction();
        transaction.add(add_spot_ix);
        transaction.add(ixCacheRootBank);
        transaction.add(ixupdateRootBank);
        transaction.feePayer = this.authority.publicKey;
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;

        await sendAndConfirmTransaction(
            this.conn,
            transaction,
            [this.authority],
            { commitment: 'confirmed', maxRetries: 100 }
        )
    }

    public async getMangoGroup(mangoCookie: MangoCookie) {
        return this.mangoClient.getMangoGroup(mangoCookie.mangoGroup)
    }

    private async initPerpMarket(mangoCookie: MangoCookie, token: MangoTokenData) {
        const maxNumEvents = 256;
        // const perpMarketPk = await this.createAccountForMango(
        //     PerpMarketLayout.span,
        // );

        const [perpMarketPk] = await PublicKey.findProgramAddress(
            [
              mangoCookie.mangoGroup.toBytes(),
              new Buffer('PerpMarket', 'utf-8'),
              token.priceOracle.publicKey.toBytes(),
            ],
            this.mangoProgramId,
          );

        const eventQ = await this.createAccountForMango(
            PerpEventQueueHeaderLayout.span + maxNumEvents * PerpEventLayout.span,
        );

        const bids = await this.createAccountForMango(
            BookSideLayout.span,
        );
        const mangoMint = token.marketIndex == 0 ? token.mint : mangoCookie.tokens[0][1].mint;

        const asks = await this.createAccountForMango(
            BookSideLayout.span,
        );

        const [mngoVaultPk] = await PublicKey.findProgramAddress(
            [
              perpMarketPk.toBytes(),
              TOKEN_PROGRAM_ID.toBytes(),
              mangoMint.toBytes(),
            ],
            this.mangoProgramId,
          );

        const instruction = await makeCreatePerpMarketInstruction(
            this.mangoProgramId,
            mangoCookie.mangoGroup,
            token.priceOracle.publicKey,
            perpMarketPk,
            eventQ,
            bids,
            asks,
            mangoMint,
            mngoVaultPk,
            this.authority.publicKey,
            mangoCookie.signerKey,
            I80F48.fromNumber(10),
            I80F48.fromNumber(5),
            I80F48.fromNumber(0.05),
            I80F48.fromNumber(0),
            I80F48.fromNumber(0.005),
            new BN(1000),
            new BN(1000),
            I80F48.fromNumber(1),
            I80F48.fromNumber(200),
            new BN(3600),
            new BN(0),
            new BN(2),
            new BN(2),
            new BN(0),
            new BN(6)
        )
        
        const transaction = new Transaction();
        transaction.add(instruction);

        await this.mangoClient.sendTransaction(transaction, this.authority, [], 3600, 'confirmed');
        //await sendAndConfirmTransaction(this.conn, transaction, additionalSigners);
        const perpMarketData: PerpMarketData = {
            publicKey: perpMarketPk,
            asks: asks,
            bids: bids,
            eventQ: eventQ,
        }
        return perpMarketData;
    }

    public async createAndInitPerpMarket(mangoCookie: MangoCookie, token: MangoTokenData): Promise<PerpMarketData> {
        const blockHash = await this.conn.getLatestBlockhashAndContext('confirmed')
        const signature = await this.mangoClient.addPerpMarket(
            await this.getMangoGroup(mangoCookie),
            token.priceOracle.publicKey,
            token.mint,
            this.authority,
            10,
            5,
            0.05,
            0,
            0.0005,
            1000,
            1000,
            256,
            1,
            200,
            3600,
            0,
            2,
        );
        this.conn.confirmTransaction({ blockhash: blockHash.value.blockhash, lastValidBlockHeight: blockHash.value.lastValidBlockHeight, signature: signature }, 'confirmed')

        const mangoGroup = await this.mangoClient.getMangoGroup(mangoCookie.mangoGroup);
        const tokenIndex = token.marketIndex;
        const perpMarketPk = mangoGroup.perpMarkets[tokenIndex].perpMarket
        const perpMarket = await this.mangoClient.getPerpMarket(perpMarketPk, token.nbDecimals, 6);
        // const perpMarketData : PerpMarketData = {
        //     publicKey: perpMarketPk,
        //     asks: perpMarket.asks,
        //     bids: perpMarket.bids,
        //     eventQ: perpMarket.eventQueue,
        // }

        const perpMarketData: PerpMarketData = {
            publicKey: PublicKey.default,
            asks: PublicKey.default,
            bids: PublicKey.default,
            eventQ: PublicKey.default,
        }
        return perpMarketData
    }

    public async refreshTokenCache(mangoCookie: MangoCookie, tokenData: MangoTokenData) {

        let ixupdateRootBank = mango_client_v3.makeUpdateRootBankInstruction(this.mangoProgramId,
            mangoCookie.mangoGroup,
            mangoCookie.mangoCache,
            tokenData.rootBank,
            [tokenData.nodeBank]);

        const transaction = new Transaction();
        transaction.add(ixupdateRootBank);
        transaction.feePayer = this.authority.publicKey;
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;
        // Sign transaction, broadcast, and confirm
        await sendAndConfirmTransaction(
            this.conn,
            transaction,
            [this.authority],
            { commitment: 'confirmed' },
        );
    }


    public async refreshRootBankCache(mangoContext: MangoCookie) {

        let ixCacheRootBank = mango_client_v3.makeCacheRootBankInstruction(this.mangoProgramId,
            mangoContext.mangoGroup,
            mangoContext.mangoCache,
            Array.from(mangoContext.tokens).map(x => x[1].rootBank));

        const transaction = new Transaction();
        transaction.add(ixCacheRootBank);
        transaction.feePayer = this.authority.publicKey;
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;
        // Sign transaction, broadcast, and confirm
        await sendAndConfirmTransaction(
            this.conn,
            transaction,
            [this.authority],
            { commitment: 'confirmed' },
        );
    }

    async refreshAllTokenCache(mangoContext: MangoCookie) {
        await this.refreshRootBankCache(mangoContext);
        await Promise.all(
            Array.from(mangoContext.tokens).map(x => this.refreshTokenCache(mangoContext, x[1]))
        );
    }

    async createSpotOpenOrdersAccount(mangoContext: MangoCookie, mangoAccount: PublicKey, owner: Keypair, tokenData: TokenData): Promise<PublicKey> {

        let mangoGroup = await this.mangoClient.getMangoGroup(mangoContext.mangoGroup);
        const marketIndex = new BN(mangoGroup.tokens.findIndex(x => x.mint.equals(tokenData.mint)));

        const [spotOpenOrdersAccount, _bump] = await PublicKey.findProgramAddress(
            [
                mangoAccount.toBuffer(),
                marketIndex.toBuffer("le", 8),
                Buffer.from("OpenOrders"),
            ], this.mangoProgramId);

        const space = OpenOrders.getLayout(this.dexProgramId).span;
        //await this.createAccount( spotOpenOrdersAccount, owner, DEX_ID, space);
        const lamports = await this.conn.getMinimumBalanceForRentExemption(space);

        let ix2 = mango_client_v3.makeCreateSpotOpenOrdersInstruction(
            this.mangoProgramId,
            mangoContext.mangoGroup,
            mangoAccount,
            owner.publicKey,
            this.dexProgramId,
            spotOpenOrdersAccount,
            tokenData.market.address,
            mangoContext.signerKey,
        )
        await this.processInstruction(ix2, [owner]);

        return spotOpenOrdersAccount;
    }

    async processInstruction(ix: anchor.web3.TransactionInstruction, signers: Array<Signer>) {
        const transaction = new Transaction();
        transaction.add(ix);
        transaction.feePayer = this.authority.publicKey;
        signers.push(this.authority);
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;
        // Sign transaction, broadcast, and confirm
        try {
            await this.mangoClient.sendTransaction(transaction,
                this.authority,
                [],
                3600,
                'confirmed');

        }
        catch (ex) {
            const ext = ex as anchor.web3.SendTransactionError;
            if (ext != null) {
                console.log("Error processing instruction : " + ext.message)
            }
            throw ex;
        }
    }

    convertCookie2Json(mangoCookie: MangoCookie, cluster: Cluster) {
        const oracles = Array.from(mangoCookie.tokens).map(x => {
            const oracle: OracleConfig = {
                publicKey: x[1].priceOracle.publicKey,
                symbol: x[0] + 'USDC'
            }
            return oracle;
        })

        const perpMarkets = Array.from(mangoCookie.tokens).map(x => {
            const perpMarket: PerpMarketConfig = {
                publicKey: x[1].perpMarket.publicKey,
                asksKey: x[1].perpMarket.asks,
                bidsKey: x[1].perpMarket.bids,
                eventsKey: x[1].perpMarket.eventQ,
                marketIndex: x[1].marketIndex,
                baseDecimals: x[1].nbDecimals,
                baseSymbol: x[0].toString(),
                name: (x[0] + 'USDC PERP'),
                quoteDecimals: 6,
            }
            return perpMarket
        })

        const spotMarkets = Array.from(mangoCookie.tokens).map(x => {

            const spotMarket: SpotMarketConfig = {
                name: x[0] + 'USDC spot Market',
                marketIndex: x[1].marketIndex,
                publicKey: x[1].market.address,
                asksKey: x[1].market.asksAddress,
                bidsKey: x[1].market.bidsAddress,
                baseDecimals: x[1].nbDecimals,
                baseSymbol: x[0].toString(),
                eventsKey: x[1].market.decoded.eventQueue,
                quoteDecimals: 6,
            }
            return spotMarket;
        })

        const tokenConfigs = Array.from(mangoCookie.tokens).map(x => {
            const tokenConfig: TokenConfig = {
                decimals: x[1].nbDecimals,
                mintKey: x[1].mint,
                nodeKeys: [x[1].nodeBank],
                rootKey: x[1].rootBank,
                symbol: x[0].toString(),
            }
            return tokenConfig
        })

        // quote token config
        tokenConfigs.push(
            {
                decimals: 6,
                mintKey: mangoCookie.usdcMint,
                rootKey: mangoCookie.usdcRootBank,
                nodeKeys: [mangoCookie.usdcNodeBank],
                symbol: "USDC",
            }
        )

        const groupConfig: GroupConfig = {
            cluster,
            mangoProgramId: this.mangoProgramId,
            name: cluster,
            publicKey: mangoCookie.mangoGroup,
            quoteSymbol: "USDC",
            oracles: oracles,
            serumProgramId: this.dexProgramId,
            perpMarkets: perpMarkets,
            spotMarkets: spotMarkets,
            tokens: tokenConfigs,
        }
        groupConfig["cacheKey"] = mangoCookie.mangoCache

        const groupConfigs: GroupConfig[] = [groupConfig]
        const cluster_urls: Record<Cluster, string> = {
            "devnet": "https://mango.devnet.rpcpool.com",
            "localnet": "http://127.0.0.1:8899",
            "mainnet": "https://mango.rpcpool.com/946ef7337da3f5b8d3e4a34e7f88",
            "testnet": "http://api.testnet.rpcpool.com"
        };

        const config = new Config(cluster_urls, groupConfigs);
        return config.toJson();
    }

    async json2Cookie(json: any, cluster: Cluster) : Promise<MangoCookie> {
        const clusterConfig = new MangoConfig(json)
        const group = clusterConfig.getGroup(cluster, cluster);
        let mangoCookie: MangoCookie = {
            mangoGroup: null,
            signerKey: null,
            mangoCache: null,
            usdcRootBank: null,
            usdcNodeBank: null,
            usdcVault: null,
            tokens: new Array<[String, MangoTokenData]>(),
            usdcMint: null,
            MSRM: null,
        };
        const mangoGroup = await this.mangoClient.getMangoGroup(group.publicKey);

        let quoteToken = mangoGroup.getQuoteTokenInfo();
        const quoteRootBankIndex = await mangoGroup.getRootBankIndex(quoteToken.rootBank);
        const rootBanks = await (await mangoGroup.loadRootBanks(this.conn));
        const quoteRootBank = rootBanks[quoteRootBankIndex];
        const quoteNodeBanks = await quoteRootBank.loadNodeBanks(this.conn);

        mangoCookie.mangoGroup = group.publicKey;
        mangoCookie.mangoCache = mangoGroup.mangoCache;
        mangoCookie.usdcRootBank = quoteToken.rootBank;
        mangoCookie.usdcNodeBank = quoteNodeBanks[0].publicKey;
        mangoCookie.usdcVault = quoteNodeBanks[0].vault;
        mangoCookie.usdcMint = mangoGroup.getQuoteTokenInfo().mint;
        const rootBanksF = rootBanks.filter(x => x != undefined);
        mangoCookie.tokens = mangoGroup.tokens.map((x, index) => {
            if (x.rootBank.equals(PublicKey.default)) {
                return ["", null]
            }
            let rootBank = rootBanksF.find(y => y.publicKey.equals(x.rootBank))
            let tokenData : MangoTokenData = {
                market: null,
                marketIndex: index,
                mint: x.mint,
                nbDecimals: 6,
                rootBank: x.rootBank,
                nodeBank: rootBank.nodeBanks[0],
                perpMarket: null,
                priceOracle: null,
                startingPrice: 0
            };
            return ["", tokenData]
        })
        return mangoCookie
    }

    async createUser(
        mangoCookie, 
        mangoGroup: mango_client_v3.MangoGroup, 
        usdcAcc: PublicKey,
        rootBanks : mango_client_v3.RootBank[],
        nodeBanks : mango_client_v3.NodeBank[],
        fundingAccounts : PublicKey[],
        authority: Keypair,
    ): Promise<MangoUser> {
        const user = Keypair.generate();

        // transfer 1 sol to the user
        {
            const ix = SystemProgram.transfer({
                fromPubkey: authority.publicKey,
                lamports : LAMPORTS_PER_SOL,
                programId: anchor.web3.SystemProgram.programId,
                toPubkey: user.publicKey,
            })
            await anchor.web3.sendAndConfirmTransaction( this.conn, new Transaction().add(ix), [authority]);
        }

        const mangoAcc = await this.mangoClient.createMangoAccount(
            mangoGroup,
            user,
            1,
            user.publicKey,
        );

        //const mangoAccount = await this.mangoClient.getMangoAccount(user.mangoAddress, this.dexProgramId)
        const depositUsdcIx = makeDepositInstruction(
            this.mangoProgramId,
            mangoCookie.mangoGroup,
            this.authority.publicKey,
            mangoGroup.mangoCache,
            mangoAcc,
            mangoCookie.usdcRootBank,
            mangoCookie.usdcNodeBank,
            mangoCookie.usdcVault,
            usdcAcc,
            new BN(10_000_000_000),
        );
        const blockHashInfo = await this.conn.getLatestBlockhashAndContext();
        const transaction = new Transaction()
            .add(depositUsdcIx)
        transaction.recentBlockhash = blockHashInfo.value.blockhash;
        transaction.feePayer = this.authority.publicKey;
        await this.mangoClient.sendTransaction(transaction, this.authority, [], 3600, 'confirmed')
        for (const tokenIte of mangoCookie.tokens)
        {
            if (tokenIte[1] === null)
            {
                continue;
            }
            const marketIndex = tokenIte[1].marketIndex;
            
            const deposit = makeDepositInstruction(
                this.mangoProgramId,
                mangoCookie.mangoGroup,
                this.authority.publicKey,
                mangoGroup.mangoCache,
                mangoAcc,
                rootBanks[marketIndex].publicKey,
                nodeBanks[marketIndex].publicKey,
                nodeBanks[marketIndex].vault,
                fundingAccounts[marketIndex],
                new BN(1_000_000_000),
            );
            const blockHashInfo = await this.conn.getLatestBlockhashAndContext();
            const transaction = new Transaction()
                .add(deposit)
            transaction.recentBlockhash = blockHashInfo.value.blockhash;
            transaction.feePayer = this.authority.publicKey;
            await this.mangoClient.sendTransaction(transaction, this.authority, [], 3600, 'confirmed')
        }

        return { kp: user, mangoAddress: mangoAcc };
    }

    public async createAndMintUsers(mangoCookie: MangoCookie, nbUsers: number, authority : Keypair): Promise<MangoUser[]> {
        const mangoGroup = await this.getMangoGroup(mangoCookie)
        const rootBanks = await mangoGroup.loadRootBanks(this.conn)
        const nodeBanksList = await Promise.all(rootBanks.map(x => x != undefined ? x.loadNodeBanks(this.conn): Promise.resolve(undefined)))
        const nodeBanks = nodeBanksList.map(x => x!=undefined ? x[0] : undefined);
        const usdcAcc = await this.mintUtils.createTokenAccount(mangoCookie.usdcMint, this.authority, this.authority.publicKey);
        await splToken.mintTo(
            this.conn,
            this.authority,
            mangoCookie.usdcMint,
            usdcAcc,
            this.authority,
            10_000_000_000 * nbUsers,
        )
        const tmpAccounts : PublicKey []= new Array(mangoCookie.tokens.length)
        for (const tokenIte of mangoCookie.tokens) {
            if (tokenIte[1] === null)
            {
                continue;
            }
            const acc = await this.mintUtils.createTokenAccount(tokenIte[1].mint, this.authority, this.authority.publicKey);
            await splToken.mintTo(
                this.conn,
                this.authority,
                tokenIte[1].mint,
                acc,
                this.authority,
                1_000_000_000 * nbUsers,
            )
            tmpAccounts[tokenIte[1].marketIndex] = acc
        }
        
        // create in batches of 10
        let users : MangoUser[] = [];
        for (let i=0; i<Math.floor((nbUsers/10)); ++i) {
            let users_batch = await Promise.all( [...Array(10)].map(_x => this.createUser(mangoCookie, mangoGroup, usdcAcc, rootBanks, nodeBanks, tmpAccounts, authority)));
            users = users.concat(users_batch)
        }
        if (nbUsers%10 != 0) {
            let last_batch = await Promise.all( [...Array(nbUsers%10)].map(_x => this.createUser(mangoCookie, mangoGroup, usdcAcc, rootBanks, nodeBanks, tmpAccounts, authority)));
            users = users.concat(last_batch)
        }
        return users;
    }
}
