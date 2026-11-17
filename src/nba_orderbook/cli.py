import argparse
import asyncio
import json
import os
from pathlib import Path
import tomllib
from .model import group_games, peak_concurrency
from .polymarket import Polymarket
from .collector import collect_batch
from .scheduler import Store, scan, submit

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',default=os.environ.get('NBA_CONFIG','/etc/nba-orderbook/config.toml'))
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('discover')
    commands.add_parser('plan')
    s=commands.add_parser('scheduler');s.add_argument('--submit')
    c=commands.add_parser('collect');c.add_argument('--batch',required=True);c.add_argument('--output',default='/kaggle/working/nba-data')
    commands.add_parser('status')
    args=parser.parse_args()
    if args.command=='collect':
        from .kaggle import Backend
        batch=json.loads(Path(args.batch).read_text())
        result=asyncio.run(collect_batch(batch,Path(args.output),Backend(batch['owner'])))
    elif args.command in ('discover','plan'):
        async def discover():
            api=Polymarket()
            try:
                return await api.discover()
            finally:
                await api.close()
        games,anomalies=asyncio.run(discover())
        result={'games':[g.to_dict() for g in games],'anomalies':anomalies}
        if args.command=='plan':
            groups=group_games(games)
            result.update(batches=[[g.slug for g in group] for group in groups],required_peak_notebooks=peak_concurrency(groups))
    else:
        config=tomllib.loads(Path(args.config).read_text());config['_path']=args.config
        if args.command=='status':
            result=json.loads((Path(config['state_dir'])/'state.json').read_text())
        else:
            from .kaggle import Backend
            backend=Backend(config['owner'])
            if args.submit:
                result=submit(config,backend,args.submit)
            else:
                async def run():
                    api=Polymarket()
                    try:
                        return await scan(config,backend,api)
                    finally:
                        await api.close()
                result=asyncio.run(run())
    print(json.dumps(result,ensure_ascii=False,default=str,indent=2))

if __name__=='__main__':
    main()
