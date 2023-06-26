import { Cluster, Config } from "@blockworks-foundation/mango-client";
import { Connection, Keypair, PublicKey } from "@solana/web3.js";
import { readFileSync, writeFileSync }  from 'fs';
import { MangoUtils } from "./utils/mango_utils";
import { getProgramMap } from "./utils/config"

if (process.argv.length < 4) {
    console.log("please enter arguments as follows\n `ts-node create_users number_of_users output_file cluster_config_file(o)`");
}

export async function main(
    file: any,
    nbUsers: number,
    authority: Keypair,
    outFile: string,
) {
    const cluster = (process.env.CLUSTER || 'localnet') as Cluster;

    const programNameToId = getProgramMap(cluster);
    const endpoint = process.env.ENDPOINT_URL || 'http://0.0.0.0:8899';
    const connection = new Connection(endpoint, 'confirmed');
    console.log('Connecting to cluster ' + endpoint)
    const mangoProgramId = new PublicKey(programNameToId['mango'])
    const dexProgramId = new PublicKey(programNameToId['serum_dex']);
    const pythProgramId = new PublicKey(programNameToId['pyth_mock']);
    const json = JSON.parse(file);
    try {
        const mangoUtils = new MangoUtils(connection, authority, mangoProgramId, dexProgramId, pythProgramId);
        let mangoCookie = await mangoUtils.json2Cookie(json, cluster);
        const users = (await mangoUtils.createAndMintUsers(mangoCookie, nbUsers, authority)).map(x => {
            const info = {};
            info['publicKey'] = x.kp.publicKey.toBase58();
            info['secretKey'] = Array.from(x.kp.secretKey);
            info['mangoAccountPks'] = [x.mangoAddress.toBase58()];
            return info;
        })
        console.log('created ' + nbUsers + ' Users');
        
        writeFileSync(outFile, JSON.stringify(users));
    }
    catch (e) {
        console.log('failed to create error ' + e);
    }
}

const configFile = process.argv.length >= 5 ? process.argv[4] : 'ids.json';
const file = readFileSync(configFile, 'utf-8');

const authority = Keypair.fromSecretKey(
    Uint8Array.from(
        JSON.parse(
            process.env.KEYPAIR ||
                readFileSync('authority.json', 'utf-8'),
        ),
    ),
);
let nbUsers = +process.argv[2];
let outFile = process.argv[3];
main(file, nbUsers, authority, outFile).then(x=> {
    console.log("finished");
})