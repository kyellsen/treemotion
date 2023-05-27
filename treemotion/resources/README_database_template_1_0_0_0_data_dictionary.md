# Datenbank-Struktur und Data-Dictionary

Version: 1_0_0_0
Passend zu: database_template_1_0_0_0.db
Diese Datei enthält eine Beschreibung aller Tabellen, deren Spalten inklusive Bedeutung und Einheiten.


## TMS-Datentabellen (z. B. 1_data_raw_1_messung)

Alle Tabellen mit der oben genannten Bezeichnung stellen einen Messdaten-Satz eines Sensors dar.

Die Version ist beim ersten Import default="raw".

Andere Versionen sind i.d.R. individuell durch den Nutzer angepasst.

Bei manueller Bearbeitung oder Löschung dieser Tabellen unbedingt die Tabelle "Daten" ebenfalls anpassen.

| Spalte                                                       | Typ     | Einheit | Beschreibung                                             | Anmerkung |
|--------------------------------------------------------------|---------|---------|----------------------------------------------------------|-----------|
| Time                                                         | DATETIME| YYYY-MM-DD HH:MM:SS.SSSSSS      | Zeitstempel, 20 Hertz                                    | NA        |
| East-West-Inclination                                        | FLOAT   | Grad    | Ost-West-Neigung                                         | NA        |
| North-South-Inclination                                      | FLOAT   | Grad    | Nord-Süd-Neigung                                         | NA        |
| Absolute-Inclination                                         | FLOAT   | Grad    | Absolute Neigung                                         | NA        |
| Inclination direction of the tree                            | FLOAT   | Grad    | Neigungsrichtung des Baumes                              | NA        |
| Temperature                                                  | FLOAT   | °C      | Temperatur                                               | NA        |
| East-West-Inclination - drift compensated                    | FLOAT   | Grad    | Temperatur-Drift kompensierte Ost-West-Neigung           | NA        |
| North-South-Inclination - drift compensated                  | FLOAT   | Grad    | Temperatur-Drift kompensierte Nord-Süd-Neigung           | NA        |
| Absolute-Inclination - drift compensated                     | FLOAT   | Grad    | Temperatur-Drift kompensierte absolute Neigung           | NA        |
| Inclination direction of the tree - drift compensated        | FLOAT   | Grad    | Temperatur-Drift kompensierte Neigungsrichtung des Baumes | NA        |

## Baum
| Spalte | Typ | Einheit | Beschreibung                         | Anmerkung |
| ------ | --- | ------- |--------------------------------------| --------- |
| id_baum | INTEGER | id | Eindeutige ID des Baumes             | NA |
| datum_erhebung | TIMESTAMP | NA | Datum der Erfassung des Baumes       | NA |
| id_baumart | INTEGER | id | Referenziert die Baumart des Baumes  | NA |
| umfang | INTEGER | cm | Umfang des Baumes auf 1 m über Boden | NA |
| hoehe | INTEGER | cm | Höhe des Baumes                      | NA |
| zwiesel_hoehe | INTEGER | cm | Höhe des Zwiesels des Baumes         | NA |

## Baumart
| Spalte | Typ | Einheit | Beschreibung               | Anmerkung |
| ------ | --- | ------- |----------------------------| --------- |
| id_baumart | INTEGER | id | Eindeutige ID der Baumart  | NA |
| baumart | VARCHAR | NA | Wissenschaftlicher Artname | NA |

## BaumBehandlung
| Spalte | Typ | Einheit | Beschreibung                                                  | Anmerkung |
| ------ | --- | ------- |---------------------------------------------------------------| --------- |
| id_baum_behandlung | INTEGER | id | Eindeutige ID der Baumbehandlung                              | NA |
| id_baum | INTEGER | id | Referenziert den behandelten Baum                             | NA |
| datum_aufbau | TIMESTAMP | NA | Datum des Beginns der Behandlung                              | NA |
| datum_abbau | TIMESTAMP | NA | Datum des Endes der Behandlung                                | NA |
| id_baum_behandlungsvariante | INTEGER | id | Referenziert die Variante der Baumbehandlung                  | NA |
| id_baum_kronensicherung | INTEGER | id | Referenziert die Eingehschaften der verbauten Kronensicherung | NA |

## BaumBehandlungsVariante
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_baum_behandlungs_variante | INTEGER | id | Eindeutige ID der Baumbehandlungsvariante | NA |
| baum_behandlungs_variante | VARCHAR | NA | Bezeichnung der Baumbehandlungsvariante | NA |
| anmerkung | VARCHAR | NA | Zusätzliche Anmerkungen zur Baumbehandlungsvariante | NA |

## BaumKronensicherung
| Spalte | Typ | Einheit | Beschreibung                                 | Anmerkung |
| ------ | --- | ------- |----------------------------------------------| --------- |
| id_baum_kronensicherung | INTEGER | id | Eindeutige ID der Baumkronensicherung        | NA |
| id_baum_kronensicherung_typ | INTEGER | id | Referenziert den Typ der Baumkronensicherung | NA |
| hoehe | INTEGER | cm | Höhe der Baumkronensicherung über Boden      | NA |
| laenge | INTEGER | cm | Länge der KS                | NA |
| umfang_stamm_a | INTEGER | cm | Umfang des Stamms A an KS                    | NA |
| umfang_stamm_b | INTEGER | cm | Umfang des Stamms B an KS                    | NA |
| durchhang | INTEGER | cm | Durchhang der KS            | NA |

## BaumKronensicherungTyp
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_baum_kronensicherung_typ | INTEGER | id | Eindeutige ID des Baumkronensicherungstyps | NA |
| baum_kronensicherung_typ | VARCHAR | NA | Bezeichnung des Baumkronensicherungstyps | NA |

## Data
| Spalte        | Typ | Einheit | Beschreibung                                          | Anmerkung |
|---------------| --- | ------- |-------------------------------------------------------| --------- |
| id_data       | INTEGER | id | Eindeutige ID der Daten                               | NA |
| id_messung    | INTEGER | id | Referenziert die zugehörige Messung                   | NA |
| version       | VARCHAR | NA | Version der Daten, freiwählbarer Name                 | NA |
| table_name    | VARCHAR | NA | Name der Tabelle (auto_data_{version}id_messung_{id}) | NA |
| datetime_start | TIMESTAMP | NA | Startzeitpunkt der enthaltenen Daten                  | NA |
| datetime_end  | TIMESTAMP | NA | Endzeitpunkt der enthaltenen Daten                    | NA |
| duration      | TIMESTAMP | NA | Zeitspanne der enthaltenen Daten                      | NA |
| length        | INTEGER | NA | Anzahl der Zeilen im Datensatz                        | NA |


## Messreihe
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_messreihe | INTEGER | id | Eindeutige ID der Messreihe | NA |
| beschreibung | VARCHAR | NA | Beschreibung der Messreihe | NA |
| datum_beginn | TIMESTAMP | NA | Startdatum der Messreihe | NA |
| datum_ende | TIMESTAMP | NA | Enddatum der Messreihe | NA |
| ort | VARCHAR | NA | Ort der Messreihe | NA |
| anmerkung | VARCHAR | NA | Zusätzliche Anmerkungen zur Messreihe | NA |
| filepaths_tms | VARCHAR | NA | Dateipfade zu den TMS-Dateien | NA |
| filepaths_ls3 | VARCHAR | NA | Dateipfade zu den LS3-Dateien | NA |

## Messung
| Spalte | Typ | Einheit | Beschreibung                              | Anmerkung |
| ------ | --- | ------- |-------------------------------------------| --------- |
| id_messung | INTEGER | id | Eindeutige ID der Messung                 | NA |
| id_messreihe | INTEGER | id | Referenziert die zugehörige Messreihe     | NA |
| id_baum_behandlung | INTEGER | id | Referenziert die zugehörige Baumbehandlung | NA |
| id_sensor | INTEGER | id | Referenziert den zugehörigen Sensor       | NA |
| id_messung_status | INTEGER | id | Referenziert den Status der Messung       | NA |
| filename | VARCHAR | NA | Dateiname der Messung                     | NA |
| id_sensor_ort | INTEGER | id | Referenziert den Ort des Sensors          | NA |
| sensor_hoehe | INTEGER | cm | Höhe des Sensors                          | NA |
| sensor_umfang | INTEGER | cm | Umfang Stämmling am Sensor des Sensors    | NA |
| sensor_ausrichtung | INTEGER | Grad | Ausrichtung des Sensors gegen Nord        | NA |

## MessungStatus
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_messung_status | INTEGER | id | Eindeutige ID des Messungsstatus | NA |
| messung_status | VARCHAR | NA | Status der Messung | NA |

## Sensor
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_sensor | INTEGER | id | Eindeutige ID des Sensors | NA |
| id_sensor_typ | INTEGER | id | Referenziert den Typ des Sensors | NA |
| id_sensor_status | INTEGER | id | Referenziert den Status des Sensors | NA |
| anmerkung | VARCHAR | NA | Zusätzliche Anmerkungen zum Sensor | NA |

## SensorOrt
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_sensor_ort | INTEGER | id | Eindeutige ID des Sensororts | NA |
| sensor_ort | VARCHAR | NA | Ort des Sensors | NA |

## SensorStatus
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_sensor_status | INTEGER | id | Eindeutige ID des Sensorstatus | NA |
| sensor_status | VARCHAR | NA | Status des Sensors | NA |

## SensorTyp
| Spalte | Typ | Einheit | Beschreibung | Anmerkung |
| ------ | --- | ------- | ------------ | --------- |
| id_sensor_typ | INTEGER | id | Eindeutige ID des Sensortyps | NA |
| sensor_typ | VARCHAR | NA | Typ des Sensors | NA |
