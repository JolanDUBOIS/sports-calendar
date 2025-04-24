import hashlib
from datetime import datetime, timezone

from sqlalchemy import event, Column, Integer, String, DateTime, ForeignKey, Date
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def generate_hash_id(fields):
    return hashlib.sha256(''.join(str(field) for field in fields).encode('utf-8')).hexdigest()


class Region(Base):
    __tablename__ = 'regions'
    id = Column(String, primary_key=True)
    name = Column(String)


class Competition(Base):
    __tablename__ = 'competitions'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    abbreviation = Column(String)
    gender = Column(String, nullable=False)  # men/women
    region_id = Column(String, ForeignKey('regions.id'))
    region = relationship('Region', back_populates="competitions")
    type = Column(String)  # e.g., 'league', 'cup'

# Relationships setup for Region
Region.competitions = relationship('Competition', back_populates="region")


class Team(Base):
    __tablename__ = 'teams'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    abbreviation = Column(String)
    gender = Column(String, nullable=False)  # men/women
    venue = Column(String)
    league_id = Column(String)  # competition with type "league"

    def validate_league(self, session):
        """ Method to validate that the league exists in the competitions table """
        competition = session.query(Competition).filter_by(id=self.league_id, type='league').first()
        if not competition:
            raise ValueError(f"League '{self.league_id}' is not a valid competition id.")
        return competition

    def add_team(self, session):
        """ When a new team is added or updated, validate the league """
        self.validate_league(session)
        session.add(self)
        session.commit()


class Match(Base):
    __tablename__ = 'matches'
    id = Column(String, primary_key=True)
    match_id = Column(String, nullable=False)
    source_match_id = Column(String, nullable=False)
    date_time = Column(DateTime, nullable=False)
    date = Column(Date, nullable=False)
    time = Column(String, nullable=False)
    home_team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    away_team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    venue = Column(String)
    competition_id = Column(String, ForeignKey('competitions.id'), nullable=False)
    channels = Column(String)
    source = Column(String)
    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)

    home_team = relationship('Team', foreign_keys=[home_team_id])
    away_team = relationship('Team', foreign_keys=[away_team_id])
    competition = relationship('Competition', foreign_keys=[competition_id])

    def before_insert(self):
        """ Method to set default values and generate ID before inserting into the database """
        if self.date_time:
            self.date = self.date_time.date()
            self.time = self.date_time.time()

        self.id = generate_hash_id([self.home_team_id, self.away_team_id, self.competition_id, self.date_time, self.created_at])
        self.match_id = generate_hash_id([self.home_team_id, self.away_team_id, self.competition_id, self.date])
        self.source_match_id = generate_hash_id([self.home_team_id, self.away_team_id, self.competition_id, self.date, self.source])


class Standing(Base):
    __tablename__ = 'standings'
    id = Column(String, primary_key=True)
    standing_id = Column(String, nullable=False)
    source_standing_id = Column(String, nullable=False)
    competition_id = Column(String, ForeignKey('competitions.id'))
    team_id = Column(String, ForeignKey('teams.id'))
    position = Column(Integer, nullable=False)
    points = Column(Integer, nullable=False)
    matches_played = Column(Integer)
    wins = Column(Integer)
    draws = Column(Integer)
    losses = Column(Integer)
    goals_for = Column(Integer)
    goals_against = Column(Integer)
    goal_difference = Column(Integer)
    source = Column(String)
    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)

    competition = relationship('Competition', foreign_keys=[competition_id])
    team = relationship('Team', foreign_keys=[team_id])

    def before_insert(self):
        """ Method to set default values and generate ID before inserting into the database """
        self.id = generate_hash_id([self.competition_id, self.team_id, self.created_at])
        self.standing_id = generate_hash_id([self.competition_id, self.team_id])
        self.source_standing_id = generate_hash_id([self.competition_id, self.team_id, self.source])


@event.listens_for(Match, 'before_insert')
@event.listens_for(Standing, 'before_insert')
def handle_before_insert(mapper, connection, target):
    target.before_insert()
