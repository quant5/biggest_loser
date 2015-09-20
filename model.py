import sqlalchemy as sqla
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

engine = sqla.create_engine('mysql://root@localhost/biggest_loser')
conn = engine.connect()
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


def create_tables(engine):
    Base.metadata.create_all(engine)

def now():
    return datetime.datetime.now()


class Loser(Base):
    __tablename__ = "losers"
    id = sqla.Column(sqla.Integer, primary_key=True)
    date = sqla.Column(sqla.Date)
    rank = sqla.Column(sqla.Integer)
    ticker = sqla.Column(sqla.String(10))
    closing_price = sqla.Column(sqla.Float)
    change = sqla.Column(sqla.Float)
    pct_change = sqla.Column(sqla.Float)
    scrape_date = sqla.Column(sqla.DateTime, default=now)

    def __repr__(self):
        return '<Ticker {} on {}>'.format(self.ticker, self.date)

def get_latest_date():
    latest = session.query(Loser).order_by(Loser.date.desc()).first()
    if latest:
        return latest.date
    return None

def store_row(date, rank, ticker, closing_price, change, pct_change):
    row_info = Loser(date=date, rank=rank, ticker=ticker,
                     closing_price=closing_price,
                     change=change, pct_change=pct_change)
    session.add(row_info)
    session.commit()

def get_tickers_and_dates():
    all_losers = session.query(Loser).all()
    tickers_and_dates = []
    for loser in all_losers:
        tickers_and_dates.append((loser.ticker, loser.date, loser.id))
    return tickers_and_dates


class NextDayLoser(Base):
    __tablename__ = "next_day_losers"
    id = sqla.Column(sqla.Integer, sqla.ForeignKey("losers.id"), primary_key=True)
    ticker = sqla.Column(sqla.String(10))
    loss_date = sqla.Column(sqla.Date)
    loss_open = sqla.Column(sqla.Float)
    loss_close = sqla.Column(sqla.Float)
    loss_adj_close = sqla.Column(sqla.Float)
    loss_pct_change = sqla.Column(sqla.Float)
    loss_volume = sqla.Column(sqla.Float)
    next_date = sqla.Column(sqla.Date)
    next_open = sqla.Column(sqla.Float)
    next_close = sqla.Column(sqla.Float)
    next_adj_close = sqla.Column(sqla.Float)
    next_pct_change = sqla.Column(sqla.Float)
    next_volume = sqla.Column(sqla.Float)
    scrape_date = sqla.Column(sqla.DateTime, default=now)


def save_next_day_loser(loss_dict, next_dict, count):
    loss_pct_change, next_pct_change = None, None
    if loss_dict.get('Close'):
        loss_pct_change = 100 * (float(loss_dict['Close']) - float(loss_dict['Open'])) / float(loss_dict['Open'])
    if next_dict.get('Close'):
        next_pct_change = 100 * (float(next_dict['Close']) - float(next_dict['Open'])) / float(next_dict['Open'])
    row_info = NextDayLoser(id=count,
                            ticker=loss_dict['Symbol'], 
                            loss_date=loss_dict.get('Date'),
                            loss_open=loss_dict.get('Open'), 
                            loss_close=loss_dict.get('Close'),
                            loss_adj_close=loss_dict.get('Adj_Close'),
                            loss_pct_change=loss_pct_change,
                            loss_volume=loss_dict.get('Volume'),
                            next_date=next_dict.get('Date'),
                            next_open=next_dict.get('Open'), 
                            next_close=next_dict.get('Close'),
                            next_adj_close=next_dict.get('Adj_Close'),
                            next_pct_change=next_pct_change,
                            next_volume=next_dict.get('Volume'),
                            )
    session.add(row_info)
    session.commit()

