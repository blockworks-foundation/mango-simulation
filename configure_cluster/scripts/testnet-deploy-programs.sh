#!/usr/bin/env bash

URL_OR_MONIKER="${URL_OR_MONIKER:=testnet}"
# spl programs are already deployed to testnet
spl_soFiles=("spl_token-3.5.0.so" "spl_memo-1.0.0.so" "spl_memo-3.0.0.so" "spl_associated-token-account-1.1.1.so" "spl_feature-proposal-1.0.0.so")
soFiles=("mango.so" "serum_dex.so" "pyth_mock.so")
DEPLOY_PROGRAM="solana program deploy -u ${URL_OR_MONIKER} --use-quic"
mkdir -p program-keypairs
output=()
for program in ${soFiles[@]}; do
    programName="${program%.*}"
    keypair="./program-keypairs/${programName}.json"
    solana-keygen new --no-passphrase -so ${keypair}
    ${DEPLOY_PROGRAM} --program-id ${keypair} bin/${program}
    pubkey=$(solana-keygen pubkey ${keypair})
    output+=("\"${programName}\":\"${pubkey}\"")
done

mapFile="testnet-program-name-to-id.json"
echo "{" > ${mapFile}
last=$((${#output[@]}-1))
echo $last
for v in ${output[@]:0:$last}; do
    echo "$v," >> ${mapFile}
done
echo ${output[-1]} >> ${mapFile}
echo "}" >> ${mapFile}
