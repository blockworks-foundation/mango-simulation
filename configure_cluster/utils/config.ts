
import { GroupConfig, OracleConfig, PerpMarketConfig, SpotMarketConfig, TokenConfig, Cluster } from '@blockworks-foundation/mango-client';
import { readFileSync }  from 'fs';


export function getProgramMap(cluster : Cluster): Map<String, String> {
  var file = "";
  if (cluster == "testnet") {
      file = readFileSync('./configure_cluster/utils/testnet-program-name-to-id.json', 'utf-8');
  } else {
      file = readFileSync('./configure_cluster/utils/genesis-program-name-to-id.json', 'utf-8');
  };
  return JSON.parse(file);
}

export class Config {
    public cluster_urls: Record<Cluster, string>;
    public groups: GroupConfig[];
  
    constructor(cluster_urls: Record<Cluster, string>, groups : GroupConfig[]) {
        this.cluster_urls = cluster_urls;
        this.groups = groups;
    }

    oracleConfigToJson(o: OracleConfig): any {
        return {
        ...o,
        publicKey: o.publicKey.toBase58(),
        };
    }
  
    perpMarketConfigToJson(p: PerpMarketConfig): any {
        return {
          ...p,
          publicKey: p.publicKey.toBase58(),
          bidsKey: p.bidsKey.toBase58(),
          asksKey: p.asksKey.toBase58(),
          eventsKey: p.eventsKey.toBase58(),
        };
    }
    
    spotMarketConfigToJson(p: SpotMarketConfig): any {
        return {
          ...p,
          publicKey: p.publicKey.toBase58(),
          bidsKey: p.bidsKey.toBase58(),
          asksKey: p.asksKey.toBase58(),
          eventsKey: p.eventsKey.toBase58(),
        };
    }

    tokenConfigToJson(t: TokenConfig): any {
        return {
          ...t,
          mintKey: t.mintKey.toBase58(),
          rootKey: t.rootKey.toBase58(),
          nodeKeys: t.nodeKeys.map((k) => k.toBase58()),
        };
      }
      

    groupConfigToJson(g: GroupConfig): any {
        return {
          ...g,
          publicKey: g.publicKey.toBase58(),
          mangoProgramId: g.mangoProgramId.toBase58(),
          serumProgramId: g.serumProgramId.toBase58(),
          oracles: g.oracles.map((o) => this.oracleConfigToJson(o)),
          perpMarkets: g.perpMarkets.map((p) => this.perpMarketConfigToJson(p)),
          spotMarkets: g.spotMarkets.map((p) => this.spotMarketConfigToJson(p)),
          tokens: g.tokens.map((t) => this.tokenConfigToJson(t)),
        };
      }
  
    public toJson(): any {
      return {
        ...this,
        groups: this.groups.map((g) => this.groupConfigToJson(g)),
      };
    }
  
    public getGroup(cluster: Cluster, name: string) {
      return this.groups.find((g) => g.cluster === cluster && g.name === name);
    }
  
    public getGroupWithName(name: string) {
      return this.groups.find((g) => g.name === name);
    }
  
    public storeGroup(group: GroupConfig) {
      const _group = this.getGroup(group.cluster, group.name);
      if (_group) {
        Object.assign(_group, group);
      } else {
        this.groups.unshift(group);
      }
    }
}
  
