import * as splToken from "@solana/spl-token";
import {
    PublicKey,
    Connection,
    Keypair,
} from "@solana/web3.js";

import { Market, OpenOrders } from "@project-serum/serum";
import { PythUtils } from "./pyth_utils";
import { SerumUtils } from "./serum_utils";

export interface TokenData {
    mint : PublicKey,
    market : Market | undefined,
    startingPrice : number,
    nbDecimals: number,
    priceOracle: Keypair | undefined,
}

export class MintUtils {

    private conn: Connection;
    private authority: Keypair;

    private recentBlockhash: string;
    private pythUtils : PythUtils;
    private serumUtils : SerumUtils;


    constructor(conn: Connection, authority: Keypair, dexProgramId: PublicKey, pythProgramId: PublicKey) {
        this.conn = conn;
        this.authority = authority;
        this.recentBlockhash = "";
        this.pythUtils = new PythUtils(conn, authority, pythProgramId);
        this.serumUtils = new SerumUtils(conn, authority, dexProgramId);
    }

    async createMint(nb_decimals = 6) : Promise<PublicKey> {
        const kp = Keypair.generate();
        return await splToken.createMint(this.conn, 
            this.authority, 
            this.authority.publicKey, 
            this.authority.publicKey, 
            nb_decimals,
            kp)
    }

    public async updateTokenPrice(tokenData: TokenData, newPrice: number) {
        this.pythUtils.updatePriceAccount(tokenData.priceOracle, 
            {
                exponent: tokenData.nbDecimals,
                aggregatePriceInfo: {
                price: BigInt(newPrice),
                conf: BigInt(newPrice * 0.01),
                },
            });
    }

    public async createNewToken(quoteToken: PublicKey, nbDecimals = 6, startingPrice = 1_000_000) {
        const mint = await this.createMint(nbDecimals);
        const tokenData : TokenData = { 
            mint: mint,
            market : await this.serumUtils.createMarket({
                baseToken : mint,
                quoteToken: quoteToken,
                baseLotSize : 1000,
                quoteLotSize : 1000,
                feeRateBps : 0,
            }),
            startingPrice : startingPrice,
            nbDecimals: nbDecimals,
            priceOracle : await this.pythUtils.createPriceAccount(),
        };
        await this.updateTokenPrice(tokenData, startingPrice)
        return tokenData;
    }

    public async createTokenAccount(mint: PublicKey, payer: Keypair, owner: PublicKey) {
        const account = Keypair.generate();
        return splToken.createAccount(
            this.conn,
            payer,
            mint,
            owner,
            account
        )
    }
}