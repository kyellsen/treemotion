from utilities.imports_classes import *

logger = get_logger(__name__)

class Baum(BaseClass):
    __tablename__ = 'Baum'
    id_baum = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    datum_erhebung = Column(DateTime)
    id_baumart = Column(Integer, ForeignKey('Baumart.id_baumart', onupdate='CASCADE'))
    umfang = Column(Integer)
    hoehe = Column(Integer)
    zwiesel_hoehe = Column(Integer)

    baumart = relationship("Baumart", backref="baeume", lazy="joined")

    def __str__(self):
        return f"Baum(id={self.id_baum}, hoehe={self.hoehe}"


class Baumart(BaseClass):
    __tablename__ = 'Baumart'
    id_baumart = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    baumart = Column(String)


class BaumBehandlung(BaseClass):
    __tablename__ = 'BaumBehandlung'
    id_baum_behandlung = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_baum = Column(Integer, ForeignKey('Baum.id_baum', onupdate='CASCADE'))
    datum_aufbau = Column(DateTime)
    datum_abbau = Column(DateTime)
    id_baum_behandlungsvariante = Column(Integer, ForeignKey('BaumBehandlungsVariante.id_baum_behandlungs_variante',
                                                             onupdate='CASCADE'))
    id_baum_kronensicherung = Column(Integer,
                                     ForeignKey('BaumKronensicherung.id_baum_kronensicherung', onupdate='CASCADE'))

    baum = relationship("Baum", backref="baum_behandlungen", lazy="joined")
    baum_behandlungsvariante = relationship("BaumBehandlungsVariante", backref="baum_behandlungen", lazy="joined")
    baum_kronensicherung = relationship("BaumKronensicherung", backref="baum_behandlungen", lazy="joined")


class BaumBehandlungsVariante(BaseClass):
    __tablename__ = 'BaumBehandlungsVariante'
    id_baum_behandlungs_variante = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    baum_behandlungs_variante = Column(String)
    anmerkung = Column(String)


class BaumKronensicherung(BaseClass):
    __tablename__ = 'BaumKronensicherung'
    id_baum_kronensicherung = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_baum_kronensicherung_typ = Column(Integer, ForeignKey('BaumKronensicherungTyp.id_baum_kronensicherung_typ',
                                                             onupdate='CASCADE'))
    hoehe = Column(Integer)
    laenge = Column(Integer)
    umfang_stamm_a = Column(Integer)
    umfang_stamm_b = Column(Integer)
    durchhang = Column(Integer)

    baum_kronensicherung_typ = relationship("BaumKronensicherungTyp", backref="baeume_kronensicherung", lazy="joined")


class BaumKronensicherungTyp(BaseClass):
    __tablename__ = 'BaumKronensicherungTyp'
    id_baum_kronensicherung_typ = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    baum_kronensicherung_typ = Column(String)
