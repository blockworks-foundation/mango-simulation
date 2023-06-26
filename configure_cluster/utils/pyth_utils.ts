import * as anchor from "@project-serum/anchor";
import * as pyth from "@pythnetwork/client";
import {
    Connection,
    Keypair,
    PublicKey,
    SystemProgram,
    Transaction,
    sendAndConfirmTransaction,
} from "@solana/web3.js";

const PRICE_ACCOUNT_SIZE = 3312;

export interface Price {
    version?: number;
    type?: number;
    size?: number;
    priceType?: string;
    exponent?: number;
    currentSlot?: bigint;
    validSlot?: bigint;
    twap?: Ema;
    productAccountKey?: PublicKey;
    nextPriceAccountKey?: PublicKey;
    aggregatePriceUpdaterAccountKey?: PublicKey;
    aggregatePriceInfo?: PriceInfo;
    priceComponents?: PriceComponent[];
}

export interface PriceInfo {
    price?: bigint;
    conf?: bigint;
    status?: string;
    corpAct?: string;
    pubSlot?: bigint;
}

export interface PriceComponent {
    publisher?: PublicKey;
    agg?: PriceInfo;
    latest?: PriceInfo;
}

export interface Product {
    version?: number;
    atype?: number;
    size?: number;
    priceAccount?: PublicKey;
    attributes?: Record<string, string>;
}

export interface Ema {
    valueComponent?: bigint;
    numerator?: bigint;
    denominator?: bigint;
}
export class PythUtils {

    conn: Connection;
    authority: Keypair;
    pythProgramId: PublicKey;

    async createAccount(size : number) : Promise<Keypair> {
        const lamports = await this.conn.getMinimumBalanceForRentExemption(size);
        let address = Keypair.generate();

        const transaction = new Transaction().add(
            SystemProgram.createAccount({
                fromPubkey: this.authority.publicKey,
                newAccountPubkey: address.publicKey,
                lamports,
                space: size,
                programId : this.pythProgramId,
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
        return address;
    }

    async store(account: Keypair, offset: number, input: Buffer) {

        let keys = [
            { isSigner: true, isWritable: true, pubkey: account.publicKey },
        ];
        let offsetBN = new anchor.BN(offset); 
        
        //const instructionData = Buffer.from([235, 116, 91, 200, 206, 170, 144, 120]);
        const data = Buffer.concat([offsetBN.toBuffer("le", 8), input]);
        
        const transaction = new anchor.web3.Transaction().add(
            new anchor.web3.TransactionInstruction({
                keys,
                programId : this.pythProgramId,
                data
            }
            ),
        );
        transaction.feePayer = this.authority.publicKey;
        let hash = await this.conn.getRecentBlockhash();
        transaction.recentBlockhash = hash.blockhash;
        // Sign transaction, broadcast, and confirm
        const signature = await anchor.web3.sendAndConfirmTransaction(
            this.conn,
            transaction,
            [this.authority, account],
            { commitment: 'confirmed' },
        );
    }

    constructor(conn: Connection, authority: Keypair, pythProgramId: PublicKey) {
        this.conn = conn;
        this.authority = authority;
        this.pythProgramId = pythProgramId;
    }

    async createPriceAccount(): Promise<Keypair> {
        return this.createAccount(PRICE_ACCOUNT_SIZE);
    }

    async createProductAccount(): Promise<Keypair> {
        return this.createPriceAccount();
    }

    async updatePriceAccount(account: Keypair, data: Price) {
        const buf = Buffer.alloc(512);
        const d = getPriceDataWithDefaults(data);
        if (!d.aggregatePriceInfo || !d.twap)
        {
            return;
        }
        d.aggregatePriceInfo = getPriceInfoWithDefaults(d.aggregatePriceInfo);
        d.twap = getEmaWithDefaults(d.twap);
        writePriceBuffer(buf, 0, d);
        
        await this.store(account, 0, buf);
    }

    async updateProductAccount(account: Keypair, data: Product) {
        const buf = Buffer.alloc(512);
        const d = getProductWithDefaults(data);

        writeProductBuffer(buf, 0, d);
        await this.store(account, 0, buf);
    }
}

function writePublicKeyBuffer(buf: Buffer, offset: number, key: PublicKey) {
    buf.write(key.toBuffer().toString("binary"), offset, "binary");
}

function writePriceBuffer(buf: Buffer, offset: number, data: Price) {
    buf.writeUInt32LE(pyth.Magic, offset + 0);
    buf.writeUInt32LE(data.version, offset + 4);
    buf.writeUInt32LE(data.type, offset + 8);
    buf.writeUInt32LE(data.size, offset + 12);
    buf.writeUInt32LE(convertPriceType(data.priceType), offset + 16);
    buf.writeInt32LE(data.exponent, offset + 20);
    buf.writeUInt32LE(data.priceComponents.length, offset + 24);
    buf.writeBigUInt64LE(data.currentSlot, offset + 32);
    buf.writeBigUInt64LE(data.validSlot, offset + 40);
    buf.writeBigInt64LE(data.twap.valueComponent, offset + 48);
    buf.writeBigInt64LE(data.twap.numerator, offset + 56);
    buf.writeBigInt64LE(data.twap.denominator, offset + 64);
    writePublicKeyBuffer(buf, offset + 112, data.productAccountKey);
    writePublicKeyBuffer(buf, offset + 144, data.nextPriceAccountKey);
    writePublicKeyBuffer(
        buf,
        offset + 176,
        data.aggregatePriceUpdaterAccountKey
    );

    writePriceInfoBuffer(buf, 208, data.aggregatePriceInfo);

    let pos = offset + 240;
    for (const component of data.priceComponents) {
        writePriceComponentBuffer(buf, pos, component);
        pos += 96;
    }
}

function writePriceInfoBuffer(buf: Buffer, offset: number, info: PriceInfo) {
    buf.writeBigInt64LE(info.price, offset + 0);
    buf.writeBigUInt64LE(info.conf, offset + 8);
    buf.writeUInt32LE(convertPriceStatus(info.status), offset + 16);
    buf.writeBigUInt64LE(info.pubSlot, offset + 24);
}

function writePriceComponentBuffer(
    buf: Buffer,
    offset: number,
    component: PriceComponent
) {
    component.publisher.toBuffer().copy(buf, offset);
    writePriceInfoBuffer(buf, offset + 32, component.agg);
    writePriceInfoBuffer(buf, offset + 64, component.latest);
}

function writeProductBuffer(buf: Buffer, offset: number, product: Product) {
    let accountSize = product.size;

    if (!accountSize) {
        accountSize = 48;

        for (const key in product.attributes) {
            accountSize += 1 + key.length;
            accountSize += 1 + product.attributes[key].length;
        }
    }

    buf.writeUInt32LE(pyth.Magic, offset + 0);
    buf.writeUInt32LE(product.version, offset + 4);
    buf.writeUInt32LE(product.atype, offset + 8);
    buf.writeUInt32LE(accountSize, offset + 12);

    writePublicKeyBuffer(buf, offset + 16, product.priceAccount);

    let pos = offset + 48;

    for (const key in product.attributes) {
        buf.writeUInt8(key.length, pos);
        buf.write(key, pos + 1);

        pos += 1 + key.length;

        const value = product.attributes[key];
        buf.writeUInt8(value.length, pos);
        buf.write(value, pos + 1);
    }
}

function convertPriceType(type: string): number {
    return 1;
}

function convertPriceStatus(status: string): number {
    return 1;
}

function getPriceDataWithDefaults({
    version = pyth.Version2,
    type = 3,
    size = PRICE_ACCOUNT_SIZE,
    priceType = "price",
    exponent = 0,
    currentSlot = 0n,
    validSlot = 0n,
    twap = {},
    productAccountKey = PublicKey.default,
    nextPriceAccountKey = PublicKey.default,
    aggregatePriceUpdaterAccountKey = PublicKey.default,
    aggregatePriceInfo = {},
    priceComponents = [],
}: Price): Price {
    return {
        version,
        type,
        size,
        priceType,
        exponent,
        currentSlot,
        validSlot,
        twap,
        productAccountKey,
        nextPriceAccountKey,
        aggregatePriceUpdaterAccountKey,
        aggregatePriceInfo,
        priceComponents,
    };
}

function getPriceInfoWithDefaults({
    price = 0n,
    conf = 0n,
    status = "trading",
    corpAct = "no_corp_act",
    pubSlot = 0n,
}: PriceInfo): PriceInfo {
    return {
        price,
        conf,
        status,
        corpAct,
        pubSlot,
    };
}

function getEmaWithDefaults({
    valueComponent = 0n,
    denominator = 0n,
    numerator = 0n,
}: Ema): Ema {
    return {
        valueComponent,
        denominator,
        numerator,
    };
}

function getProductWithDefaults({
    version = pyth.Version2,
    atype = 2,
    size = 0,
    priceAccount = PublicKey.default,
    attributes = {},
}: Product): Product {
    return {
        version,
        atype,
        size,
        priceAccount,
        attributes,
    };
}
