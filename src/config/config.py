from config.types import Exchange

# noinspection SpellCheckingInspection
translate_tickers = {
    Exchange.Etoro: {
        'MDT.US': 'MDT',
        'ROG.ZU': 'ROG.SW',
        'VACQ': 'RKLB',
    },
    Exchange.Ibkr: {
        'VACQ': 'RKLB',
    },
    Exchange.IG: {
        'ARK Autonomous Technology & Robotics ETF': 'ARKQ',
        'ARK Fintech Innovation ETF': 'ARKF',
        'ARK Genomic Revolution ETF': 'ARKG',
        'ARK Innovation ETF': 'ARKK',
        'ARK Space Exploration & Innovation ETF': 'ARKX',
        'ARK Web x.0 ETF': 'ARKW',
        'Arcimoto Inc': 'FUV',
        'Berkeley Lights Inc': 'BLI',
        'Beyond Meat Inc (All Sessions)': 'BYND',
        'CRISPR Therapeutic SA': 'CRSP',
        'Editas Medicine Inc': 'EDIT',
        'Intellia Therapeutics Inc': 'NTLA',
        'Lemonade Inc': 'LMND',
        'NVIDIA Corp (All Sessions)': 'NVDA',
        'One (Ord)': 'MKFG',
        'Palantir Technologies Inc': 'PLTR',
        'Palantir Technologies Inc (All Sessions)': 'PLTR',
        'Rocket Lab USA Inc': 'RKLB',
        'SPDR S&P 500 ETF Trust (All Sessions)': 'SPY',
        'Shift4 Payments Inc': 'FOUR',
        'Tesla Motors Inc (All Sessions)': 'TSLA',
        'Vector Acquisition Corporation': 'RKLB',
    },
}
