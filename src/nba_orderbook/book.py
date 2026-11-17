"""保留完整价格深度，增量数量是价格档位的新总量。"""
from decimal import Decimal

class Book:
    def __init__(self, assets):
        self.assets = {asset:{'bids':{},'asks':{}} for asset in assets}
        self.snapshots = set()

    def apply(self, event):
        if event.get('event_type') == 'book' and event.get('asset_id') in self.assets:
            asset = event['asset_id']
            self.assets[asset] = {side:{str(Decimal(x['price']).normalize()):x['size'] for x in event.get(side,[]) if Decimal(x['size']) != 0}
                                  for side in ('bids','asks')}
            self.snapshots.add(asset)
        elif event.get('event_type') == 'price_change':
            for change in event.get('price_changes',[]):
                if change.get('asset_id') not in self.assets:
                    continue
                side = 'bids' if change['side'] == 'BUY' else 'asks'
                levels = self.assets[change['asset_id']][side]
                price = str(Decimal(change['price']).normalize())
                if Decimal(change['size']) == 0:
                    levels.pop(price,None)
                else:
                    levels[price] = change['size']
