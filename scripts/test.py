#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
from datetime import datetime, timedelta

import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row, functions as fnc
from pyspark.sql.types import BooleanType, StringType
from pyspark.sql.window import Window

reload(sys)
sys.setdefaultencoding("utf-8")
assert sys.version_info[:2] == (2, 6), "Application requires Python 2.6.x"

col = fnc.col
when = fnc.when
generate_id = fnc.monotonically_increasing_id
margines_czasowy_minuty = 10


class TableInfo(object):
    def __init__(self, name):
        self.name = name
        self.stir_table = "fd_stage.stir_" + self.name
        self.clean_table = "fd_stage.stir_clean_" + self.name
        self._clean_path = "/fd/stage/stir_clean/{0}/hive".format(name)

    def clean_path(self, partition=None):
        return self._clean_path + ("/" + partition if partition else "")


class FileInfo(object):
    def __init__(self, path, size):
        self.path = path
        self.size = size
        self.name = os.path.basename(path)


class SparkApp(object):
    def __init__(self, parallelism, thread_memory, bucket_size):
        conf = SparkConf().setAppName("Stir Mass Table Cleaning")
        conf.set("spark.sql.shuffle.partitions", str(parallelism))
        conf.set("spark.default.parallelism", str(parallelism))
        conf.set("spark.sql.parquet.writeLegacyFormat", "true")
        conf.set("spark.sql.parquet.compression.codec", "gzip")
        conf.set("spark.yarn.executor.memoryOverhead", "2048")
        conf.set("spark.sql.broadcastTimeout", "6000")
        conf.set("spark.yarn.maxAppAttempts", "1")
        conf.set("hive.exec.dynamic.partition", "true")
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        self._sc = SparkContext(conf=conf)
        self._hc = HiveContext(self._sc)
        jvm = self._sc.__getattribute__("_jvm")
        jsc = self._sc.__getattribute__("_jsc")
        driver_memory = self._sc.__getattribute__("_conf").get("spark.driver.memory", "1G")
        self._path_to_str = lambda path: str(jvm.org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(path))
        self._fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
        self._path = jvm.org.apache.hadoop.fs.Path
        self._parallelism = parallelism
        self._threads = max(1, int(driver_memory[:-1]) * (
            1024 ** 3 if driver_memory[-1] == "G" else 1024 ** 2) / thread_memory)
        self._bucket_size = bucket_size
        self.sql = self._hc.sql

    def table(self, full_table_name, alias_name=None):
        df = self._hc.table(full_table_name)
        return df.alias(alias_name) if alias_name else df

    def add_partition(self, table_name, partition, partition_file_path):
        self._hc.sql(
            "ALTER TABLE {0} ADD IF NOT EXISTS PARTITION (data_pozyskania='{1}') LOCATION '{2}'"
                .format(table_name, partition, partition_file_path))

    def to_df(self, data, *schema):
        return self._sc.parallelize(data).toDF(*schema)

    def list_files(self, path):
        return [FileInfo(self._path_to_str(fs.getPath()), fs.getLen()) for fs in self._fs.listStatus(self._path(path))]

    def file_exists(self, path):
        return self._fs.exists(self._path(path))

    def delete_file(self, path):
        if self.file_exists(path) and not self._fs.delete(self._path(path), True):
            raise IOError("Failed deletion of " + path)

    def move_file(self, src, dest):
        if self.file_exists(dest) and not self._fs.rename(self._path(dest), self._path(dest + ".backup")):
            raise IOError("Failed moving {0} to {0}.backup".format(dest))
        if not self._fs.rename(self._path(src), self._path(dest)):
            raise IOError("Failed moving {0} to {1}".format(src, dest))
        self.delete_file(dest + ".backup")


class Proces(object):
    def __init__(self, sql_func, table_func):
        self.status_start = "start"
        self.status_wznowienie = "wznowienie"
        self.status_koniec = "koniec"
        self.ladowania_log_table = TableInfo("ladowania_log")
        self.ladowania_paczki_table = TableInfo("ladowania_paczki")
        self._sql = sql_func
        self._table = table_func

    def create_tables(self):
        self._sql("""
            CREATE TABLE IF NOT EXISTS fd_stage.stir_clean_ladowania_log (
                etap string,
                numer_ladowania bigint,
                przyrost_od timestamp,
                przyrost_do timestamp,
                data_dodania timestamp
                )
            COMMENT 'STIR_DB ladowania danych oczyszczonych'
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            LOCATION '/fd/stage/stir_clean/ladowania_log/hive'
        """)

        self._sql("""
            CREATE TABLE IF NOT EXISTS fd_stage.stir_clean_ladowania_paczki (
                tabela string,
                fd_load_id string,
                numer_ladowania bigint,
                data_zakonczenia timestamp
                )
            COMMENT 'STIR_DB ladowania pojedynczych paczek dla tabel'
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            LOCATION '/fd/stage/stir_clean/ladowania_paczki/hive'
        """)

    def dodaj_log(self, status, numer_ladowania, przyrost_od, przyrost_do):
        self._sql("SELECT '{0}', {1}, '{2}', '{3}', current_timestamp()"
                  .format(status, numer_ladowania, przyrost_od, przyrost_do)) \
            .write \
            .mode("append") \
            .saveAsTable(self.ladowania_log_table.clean_table)

    def dodaj_paczke(self, tabela, fd_load_id, numer_ladowania):
        self._sql("SELECT '{0}', '{1}', {2}, current_timestamp()".format(tabela, fd_load_id, numer_ladowania)) \
            .write \
            .mode("append") \
            .saveAsTable(self.ladowania_paczki_table.clean_table)

    def proces_do_zaladowania(self):
        ostatni_log = self.ostatni_log()
        przyrost_do = datetime.now() - timedelta(minutes=margines_czasowy_minuty)

        if ostatni_log is None:
            print("Inicjalne ladowanie")
            return 0, self.status_start, "2000-01-01", przyrost_do
        elif ostatni_log.etap == self.status_koniec:
            print("Nowe ladowanie, numer: {0}, przyrost_od: {1}, przyrost_do: {2}".format(
                ostatni_log.numer_ladowania + 1, ostatni_log.przyrost_do, przyrost_do))
            return ostatni_log.numer_ladowania + 1, self.status_start, ostatni_log.przyrost_do, przyrost_do
        else:
            print("Ladowanie {0} zostaje wznowione".format(ostatni_log.numer_ladowania))
            return ostatni_log.numer_ladowania, self.status_wznowienie, ostatni_log.przyrost_od, ostatni_log.przyrost_do

    def ostatni_log(self):
        logi = self._table(self.ladowania_log_table.clean_table).orderBy(col("data_dodania").desc()).take(1)
        return logi[0] if logi else None

    def czy_zaladowano_kursy(self, numer_ladowania):
        liczba_paczek = self._table(self.ladowania_paczki_table.clean_table, "paczki") \
            .where(col("numer_ladowania") == numer_ladowania) \
            .where(col("tabela") == "nbp_kursy") \
            .count()
        return liczba_paczek == 1

    def paczki_do_zaladowania(self, numer_ladowania, data_od, data_do):
        paczki = self._table("fd_stage.stir_pliki_wejsciowe", "pliki") \
            .where((col("pliki.data_ladowania") >= data_od) & (col("pliki.data_ladowania") < data_do)) \
            .join(self._table(self.ladowania_paczki_table.clean_table, "paczki"),
                  (col("pliki.fd_load_id") == col("paczki.fd_load_id")) &
                  (col("paczki.numer_ladowania") == numer_ladowania) &
                  (col("pliki.nazwa_tabeli") == fnc.concat(fnc.lit("stir_"), col("paczki.tabela"))),
                  "left") \
            .where(col("paczki.data_zakonczenia").isNull()) \
            .groupBy("pliki.nazwa_tabeli") \
            .agg(fnc.collect_set("pliki.fd_load_id").alias("fd_load_ids")) \
            .collect()
        return dict([(row.nazwa_tabeli, row.fd_load_ids[:10]) for row in paczki])  # dict(paczki)

    def czyszczenie(self):
        print("Czyszczenie po procesie")
        defragment_table(self.ladowania_log_table.clean_table, self.ladowania_log_table.clean_path(), 4)
        defragment_table(self.ladowania_paczki_table.clean_table, self.ladowania_paczki_table.clean_path(), 4)


app = SparkApp(100, 96 * 1024 * 1024, 1500)


def duplikat(cols):
    non_tech_cols = [c for c in cols if c not in ("data_pozyskania", "fd_load_id")]
    return fnc.row_number().over(Window.partitionBy(non_tech_cols).orderBy(fnc.col("data_pozyskania").desc()))


def digits_only(value):
    if isinstance(value, unicode):
        return u"".join(filter(unicode.isnumeric, value))
    return "".join(filter(str.isdigit, str(value)))


digits_only_udf = fnc.udf(digits_only, StringType())


def tel(value):
    return "+" + digits_only(value).lstrip("0") if value and value.strip() else value


tel_udf = fnc.udf(tel, StringType())


def blokady_fn(fd_load_id, numer_ladowania):
    blokady = TableInfo("blokady")

    blokady_table = app.table(blokady.stir_table)

    partycje = blokady_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(blokady_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        blokady_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .select(
                "id_banku", "dat_danych", "numer", "numer_org", "nip", "nip_org", "ref_banku", "rodzaj", "kwota",
                "waluta", "dat_rej", "dat_obow", "dat_zamk", "organ", "rodz_rach", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(blokady.clean_path(partycja))

        app.add_partition(blokady.clean_table, partycja, blokady.clean_path(partycja))


def kartoteki_osoba_fn(fd_load_id, numer_ladowania):
    kartoteki_osoba = TableInfo("kartoteki_osoba")
    kartoteki_podmiot = TableInfo("kartoteki_podmiot")

    kartoteki_osoba_table = app.table(kartoteki_osoba.stir_table)

    partycje = kartoteki_osoba_table \
        .where(col("fd_load_id") == fd_load_id) \
        .select(kartoteki_osoba_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = kartoteki_osoba_table.where(
            (col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)).limit(100)
        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter(col("is_duplicate") == 1) \
            .drop("is_duplicate") \
            .withColumnRenamed("pesel", "pesel_org") \
            .withColumn("pesel", digits_only_udf(col("pesel_org"))) \
            .withColumnRenamed("dok_nr", "dok_nr_org") \
            .withColumn("dok_nr", digits_only_udf(col("dok_nr_org"))) \
            .withColumnRenamed("tel", "tel_org") \
            .withColumn("tel", tel_udf(col("tel_org"))) \
            .alias("ko") \
            .join(app.table(kartoteki_podmiot.stir_table, "kp"), col("ko.id_podmiot") == col("kp.id_podmiot"), "left") \
            .withColumn("data_zakonczenia", when(col("kp.status") == "Z", col("kp.dat_danych"))) \
            .select("ko.id_podmiot", "ko.dat_danych", "ko.typ", "ko.pesel", "ko.pesel_org", "ko.nazwisko", "ko.imie",
                    "ko.kraj_urodz", "ko.dat_urodz",
                    "ko.obywat", "ko.kraj", "ko.k_poczt", "ko.miejsc", "ko.ul", "ko.nr_domu", "ko.nr_lokalu",
                    "ko.tel_org",
                    "ko.tel", "ko.email",
                    "ko.dok_rodz", "ko.dok_nr", "dok_nr_org", "ko.dat_rel", "ko.poz_upr", "ko.ref_banku",
                    "ko.zakr_pelnom", "data_zakonczenia", "ko.fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(kartoteki_osoba.clean_path(partycja))

        app.add_partition(kartoteki_osoba.clean_table, partycja, kartoteki_osoba.clean_path(partycja))


def kartoteki_podmiot_fn(fd_load_id, numer_ladowania):
    kartoteki_podmiot = TableInfo("kartoteki_podmiot")
    kartoteki_podmiot_table = app.table(kartoteki_podmiot.stir_table)

    partycje = kartoteki_podmiot_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(kartoteki_podmiot_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = kartoteki_podmiot_table.where(
            (col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)).limit(100)
        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter(col("is_duplicate") == 1) \
            .drop("is_duplicate") \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .withColumnRenamed("pesel", "pesel_org") \
            .withColumn("pesel", digits_only_udf(col("pesel_org"))) \
            .withColumnRenamed("tel", "tel_org") \
            .withColumn("tel", tel_udf(col("tel_org"))) \
            .select("id_podmiot", "dat_danych", "status", "nip_org", "nip", "pesel", "pesel_org", "nazwa",
                    "kraj_r_podat",
                    "regon", "krs", "rodz_dzial", "dat_rej", "s_kraj", "s_k_poczt", "s_miejsc", "s_ul", "s_nr_domu",
                    "s_nr_lokalu", "k_kraj", "k_k_poczt", "k_miejsc", "k_ul", "k_nr_domu", "k_nr_lokalu", "d_kraj",
                    "d_k_poczt", "d_miejsc", "d_ul", "d_nr_domu", "d_nr_lokalu", "tel", "tel_org", "email", "id_stir",
                    "id_banku", "ref_banku", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(kartoteki_podmiot.clean_path(partycja))

        app.add_partition(kartoteki_podmiot.clean_table, partycja, kartoteki_podmiot.clean_path(partycja))


def kartoteki_rachunek_fn(fd_load_id, numer_ladowania):
    kartoteki_rachunek = TableInfo("kartoteki_rachunek")
    kartoteki_podmiot = TableInfo("kartoteki_podmiot")
    relacje_vat_inicjalne = TableInfo("relacje_vat_inicjalne")
    operacje_transakcje = TableInfo("operacje_transakcje")
    operacje_mas_transakcje = TableInfo("operacje_mas_transakcje")

    kartoteki_rachunek_table = app.table(kartoteki_rachunek.stir_table, "kr")

    partycje = kartoteki_rachunek_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(kartoteki_rachunek_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = kartoteki_rachunek_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100)
        dfn = df.select("numer")
        numer_rodzaj_hist = dfn \
            .join(app.table(kartoteki_rachunek.stir_table, "kr_join"), dfn.numer == col("kr_join.numer"), "left") \
            .groupBy(dfn.numer) \
            .agg(fnc.collect_set("kr_join.rodz_rach")) \
            .collect()

        numer_rodzaj_hist_dict = dict(numer_rodzaj_hist)

        rodzaje_rachunku = dfn \
            .join(
                app.table(relacje_vat_inicjalne.stir_table, "v"),
                (dfn.numer == col("v.numer")) | (dfn.numer == col("v.rach_vat")),
                "left") \
            .withColumn(
                "rodzaj_rachunku_cln",
                when(dfn.numer == col("v.numer"), "ROZL").when(col("v.rach_vat") == dfn.numer, "RVAT")) \
            .groupBy(dfn.numer) \
            .agg(fnc.collect_set("rodzaj_rachunku_cln")) \
            .collect()

        rodzaje_rachunku_w_vat_inicjalne_dict = dict(rodzaje_rachunku)

        dane_crp = dfn \
            .join(app.table("fd_stage.crp_rachunki_bankowe", "crp"), dfn.numer == col("crp.nr_rach"), "left") \
            .filter((col("crp.fd_status") != "D") & (col("crp.nr_rach") != "")) \
            .withColumn(
                "najnowszy",
                fnc.row_number().over(Window.partitionBy("crp.nr_rach").orderBy(col("crp.fd_data").desc()))) \
            .filter(col("najnowszy") == 1) \
            .select("crp.nr_rach") \
            .distinct() \
            .map(lambda row: row[0]) \
            .collect()

        oper = app.table(operacje_transakcje.stir_table, "oper")
        oper_mas = app.table(operacje_mas_transakcje.stir_table, "oper_mas")
        dane_operacje = dfn.join(oper, dfn.numer == oper.rach, "left") \
            .where(oper.rach != "") \
            .select(dfn.numer, oper.rach, oper.rodz_rach) \
            .unionAll(
                dfn
                .join(oper_mas, dfn.numer == oper_mas.rach, "left")
                .where(oper_mas.rach != "")
                .select(dfn.numer, oper_mas.rach, oper_mas.rodz_rach)) \
            .groupBy("rach") \
            .agg(fnc.collect_set("rodz_rach")) \
            .collect()

        rodzaje_rachunku_w_operacje_dict = dict(dane_operacje)

        rodzaje_rachunku_w_vat_inicj = fnc.udf(lambda numer: rodzaje_rachunku_w_vat_inicjalne_dict[numer], StringType())
        rodzaj_z_hist_kart = fnc.udf(
            lambda numer: all(s in (u"ROZL", u"") for s in numer_rodzaj_hist_dict[numer]),
            StringType())

        jest_w_crp = fnc.udf(lambda numer: numer in dane_crp, BooleanType())
        rodzaje_rachunku_w_operacje = fnc.udf(lambda numer: rodzaje_rachunku_w_operacje_dict[numer], StringType())

        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter(col("is_duplicate") == 1) \
            .drop("is_duplicate") \
            .join(app.table(kartoteki_podmiot.stir_table, "kp"), col("kr.id_podmiot") == col("kp.id_podmiot"), "left") \
            .withColumn("data_zamk_clean", when(col("kp.status") == "Z", col("kp.dat_danych"))) \
            .withColumn("rodzaj_rach_hist", when(rodzaj_z_hist_kart(col("numer")) == "true", "ROZL")) \
            .withColumn("rodzaj_rach_vat_inicj", rodzaje_rachunku_w_vat_inicj(col("numer"))) \
            .withColumn("rodzaj_rach_operacje", rodzaje_rachunku_w_operacje(col("numer"))) \
            .withColumn("waluta_clean",
                        when(col("kr.waluta") == "VAT", "PLN")
                        .when(col("rodzaj_rach_hist") != "", col("rodzaj_rach_hist"))
                        .when(col("rodzaj_rach_vat_inicj") != "", col("rodzaj_rach_vat_inicj"))
                        .when(col("rodzaj_rach_operacje") != "", col("rodzaj_rach_operacje"))
                        .otherwise(col("kr.waluta"))) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .withColumn("rodzaj_rach_clean", when(col("kr.waluta") == "VAT", "RVAT")) \
            .withColumn("czy_w_crp_kep", jest_w_crp(col("numer"))) \
            .select("kr.id_podmiot", "kr.dat_danych", "numer", "numer_org", "kr.waluta",
                    "kr.dat_uruch",
                    "kr.data_zamk", "kr.id_banku", "kr.rodz_rach", "kr.rach_vat", "rodzaj_rach_clean", "czy_w_crp_kep",
                    "kr.fd_load_id") \
            .distinct() \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(kartoteki_rachunek.clean_path(partycja))

        app.add_partition(kartoteki_rachunek.clean_table, partycja, kartoteki_rachunek.clean_path(partycja))


def kartoteki_relacje_osoba_rachunek_fn(fd_load_id, numer_ladowania):
    kartoteki_relacje_osoba_rachunek = TableInfo("kartoteki_relacje_osoba_rachunek")
    kartoteki_podmiot = TableInfo("kartoteki_podmiot")
    kartoteki_rachunek = TableInfo("kartoteki_rachunek")

    kartoteki_relacje_osoba_rachunek_table = app.table(kartoteki_relacje_osoba_rachunek.stir_table, "kror")

    partycje = kartoteki_relacje_osoba_rachunek_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(kartoteki_relacje_osoba_rachunek_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        kartoteki_relacje_osoba_rachunek_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .alias("kror") \
            .join(
                app.table(kartoteki_podmiot.stir_table, "kp"), col("kror.id_podmiot") == col("kp.id_podmiot"), "left") \
            .join(
                app.table(kartoteki_rachunek.stir_table, "kr"), col("kror.id_podmiot") == col("kr.id_podmiot"), "left")\
            .withColumn(
                "data_zakonczenia",
                when(col("kp.status") == "Z", col("kp.data_pozyskania"))
                .when((col("kr.data_zamk") != "") & (col("kr.data_zamk") != "1900-01-01"), col("kr.data_zamk"))) \
            .select("kror.id_podmiot", "kror.id_banku", "kror.ref_banku_p", "kror.ref_banku_o", "kror.numer",
                    "kror.numer_org", "kror.data_pelnom", "data_zakonczenia", "kror.fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(kartoteki_relacje_osoba_rachunek.clean_path(partycja))

        app.add_partition(kartoteki_relacje_osoba_rachunek.clean_table, partycja,
                          kartoteki_relacje_osoba_rachunek.clean_path(partycja))


def lokaty_fn(fd_load_id, numer_ladowania):
    lokaty = TableInfo("lokaty")

    lokaty_table = app.table(lokaty.stir_table)

    partycje = lokaty_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(lokaty_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        lokaty_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .select("id_banku", "dat_danych", "numer_org", "numer", "rodz_rach", "nip", "nip_org", "ref_banku",
                    "waluta", "dat_uruch", "dat_wazn", "dat_zamk", "status", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(lokaty.clean_path(partycja))

        app.add_partition(lokaty.clean_table, partycja, lokaty.clean_path(partycja))


def operacje_rachunki_fn(fd_load_id, numer_ladowania):
    operacje_rachunki = TableInfo("operacje_rachunki")

    operacje_rachunki_table = app.table(operacje_rachunki.stir_table)

    partycje = operacje_rachunki_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(operacje_rachunki_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = operacje_rachunki_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100)
        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter(col("is_duplicate") == 1) \
            .drop("is_duplicate") \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .select("numer", "numer_org", "nip", "nip_org", "waluta", "saldo_otw", "saldo_zamk", "l_wierszy",
                    "obrot_wn",
                    "obrot_ma", "id_banku", "rodz_rach", "kwota_zablok", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(operacje_rachunki.clean_path(partycja))

        app.add_partition(operacje_rachunki.clean_table, partycja, operacje_rachunki.clean_path(partycja))


def rach_wirt_fn(fd_load_id, numer_ladowania):
    rach_wirt = TableInfo("rach_wirt")
    rach_wirt_table = app.table(rach_wirt.stir_table)

    partycje = rach_wirt_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(rach_wirt_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        rach_wirt_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("rach", "rach_org") \
            .withColumn("rach", digits_only_udf(col("rach_org"))) \
            .withColumnRenamed("rach_wirt", "rach_wirt_org") \
            .withColumn("rach_wirt", digits_only_udf(col("rach_wirt_org"))) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .select("id_banku", "ref_banku", "nip", "nip_org", "status", "rach", "rach_org", "rach_wirt",
                    "rach_wirt_org", "dat_danych", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(rach_wirt.clean_path(partycja))

        app.add_partition(rach_wirt.clean_table, partycja, rach_wirt.clean_path(partycja))


def relacje_vat_inicjalne_fn(fd_load_id, numer_ladowania):
    relacje_vat_inicjalne = TableInfo("relacje_vat_inicjalne")
    relacje_vat_inicjalne_table = app.table(relacje_vat_inicjalne.stir_table)

    partycje = relacje_vat_inicjalne_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(relacje_vat_inicjalne_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        relacje_vat_inicjalne_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .select("id_banku", "nip", "nip_org", "ref_banku", "dat_danych",
                    "numer", "numer_org", "rach_vat", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(relacje_vat_inicjalne.clean_path(partycja))

        app.add_partition(relacje_vat_inicjalne.clean_table, partycja, relacje_vat_inicjalne.clean_path(partycja))


def risk_score_fn(fd_load_id, numer_ladowania):
    generate_risk_score_id = fnc.udf(lambda generated: "{0}_{1}".format(numer_ladowania, generated), StringType())

    risk_score = TableInfo("risk_score")
    risk_score_table = app.table(risk_score.stir_table)

    partycje = risk_score_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(risk_score_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        risk_score_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumn("generated_id", generate_id()) \
            .withColumn("risk_score_id", generate_risk_score_id("generated_id")) \
            .withColumnRenamed("nip", "nip_org") \
            .withColumn("nip", digits_only_udf(col("nip_org"))) \
            .withColumnRenamed("pesel", "pesel_org") \
            .withColumn("pesel", digits_only_udf(col("pesel_org"))) \
            .select("id_podmiot", "nip", "nip_org", "regon", "pesel", "pesel_org", "dat_oceny", "nazwa", "pkt_ryzyka",
                    "kat_ryzyka", "czyn_ryzyka", "waga_czyn", "id_stir", "ref_banku", "fd_load_id", "risk_score_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(risk_score.clean_path(partycja))

        app.add_partition(risk_score.clean_table, partycja, risk_score.clean_path(partycja))


def risk_score_rachunki_fn(fd_load_id, numer_ladowania):
    risk_score_rachunki = TableInfo("risk_score_rachunki")
    risk_score_rachunki_table = app.table(risk_score_rachunki.stir_table)

    partycje = risk_score_rachunki_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(risk_score_rachunki_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        risk_score_rachunki_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100) \
            .withColumnRenamed("numer", "numer_org") \
            .withColumn("numer", digits_only_udf(col("numer_org"))) \
            .select("id_podmiot", "numer", "numer_org", "id_banku", "fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(risk_score_rachunki.clean_path(partycja))

        app.add_partition(risk_score_rachunki.clean_table, partycja, risk_score_rachunki.clean_path(partycja))


def risk_score_czynniki_fn(fd_load_id, numer_ladowania):
    def rozbij_czynniki(row):
        czynniki = row.czyn_ryzyka.split("|")
        wagi = row.waga_czyn.split("|")
        print(row.risk_score_id)
        return zip(czynniki, wagi, [row.risk_score_id] * len(czynniki))

    risk_score = TableInfo("risk_score")
    risk_score_czynniki = TableInfo("risk_score_czynniki")

    risk_score_table = app.table(risk_score.clean_table)

    partycje = risk_score_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(risk_score_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        rdd = risk_score_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id) &
                   col("risk_score_id").isNotNull()) \
            .limit(100) \
            .flatMap(rozbij_czynniki) \
            .distinct()

        if not rdd.isEmpty():
            rdd \
                .toDF(["czyn_ryzyka", "waga_czyn", "risk_score_id"]) \
                .repartition(1) \
                .write \
                .mode("append") \
                .parquet(risk_score_czynniki.clean_path(partycja))

            app.add_partition(risk_score_czynniki.clean_table, partycja, risk_score_czynniki.clean_path(partycja))


def pelnom_z_transakcji_fn(fd_load_id, numer_ladowania):
    def unpivot(row):
        return [Row(ref_trans=row.ref_trans,
                    data_pozyskania=row.data_pozyskania,
                    ref_bank_pelnom=row["ref_bank_pelnom_" + j],
                    pesel_pelnom=row["pesel_pelnom_" + j],
                    dok_nr_pelnom=row["dok_nr_pelnom_" + j],
                    dane_pelnom=row["dane_pelnom_" + j],
                    wiersz_nr=j) for j in ("1", "2", "3")]

    operacje_transakcje = TableInfo("operacje_transakcje")
    operacje_mas_transakcje = TableInfo("operacje_mas_transakcje")
    pelnom_z_transakcji = TableInfo("pelnom_z_transakcji")

    pelnom_z_transakcji_columns = [
        "{0}_pelnom_{1}".format(column, i) for column in ("ref_bank", "pesel", "dok_nr", "dane") for i in (1, 2, 3)]
    pelnom_zrodlo_table = app.table(operacje_transakcje.stir_table) \
        .where(col("fd_load_id") == fd_load_id) \
        .limit(100) \
        .select("ref_trans", "data_pozyskania", *pelnom_z_transakcji_columns) \
        .unionAll(
            app.table(operacje_mas_transakcje.stir_table)
                .where(col("fd_load_id") == fd_load_id)
                .limit(100)
                .select("ref_trans", "data_pozyskania", *pelnom_z_transakcji_columns))

    rdd = pelnom_zrodlo_table.flatMap(unpivot)
    partycje = rdd.map(lambda row: row.data_pozyskania).distinct().collect()

    if not rdd.isEmpty():
        df = rdd.toDF(["ref_trans", "ref_bank_pelnom", "pesel_pelnom", "dok_nr_pelnom", "dane_pelnom", "wiersz_nr",
                      "data_pozyskania"])

        for partycja in partycje:
            df.where(col("data_pozyskania") == partycja) \
                .drop("data_pozyskania") \
                .repartition(1) \
                .write \
                .mode("append") \
                .parquet(pelnom_z_transakcji.clean_path(partycja))

            app.add_partition(pelnom_z_transakcji.clean_table, partycja, pelnom_z_transakcji.clean_path(partycja))


def operacje_transakcje_fn(fd_load_id, numer_ladowania):
    kartoteki_rachunek = TableInfo("kartoteki_rachunek")
    operacje_transakcje = TableInfo("operacje_transakcje")
    nbp_kursy = TableInfo("nbp_kursy")

    window = Window.partitionBy("kart.numer").orderBy(col("kart.data_pozyskania").desc(), col("kart.dat_danych").desc())
    distinct_kartoteki_rachunek = app.table(kartoteki_rachunek.stir_table, "kart") \
        .withColumn("newest_kart", fnc.row_number().over(window)) \
        .filter(col("newest_kart") == 1) \
        .drop("newest_kart")

    operacje_transakcje_table = app.table(operacje_transakcje.stir_table, "oper")

    partycje = operacje_transakcje_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(operacje_transakcje_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = operacje_transakcje_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100)

        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter((col("is_duplicate") == 1) & (col("ref_trans") != "")) \
            .drop("is_duplicate") \
            .join(distinct_kartoteki_rachunek.alias("kart"), col("oper.rach") == col("kart.numer"), "left") \
            .join(app.table(nbp_kursy.clean_table, "kurs"),
                  (((col("oper.waluta") == col("kurs.waluta")) | (col("kart.waluta") == col("kurs.waluta")))
                   & (col("oper.data_trans") == col("kurs.data_waluty"))),
                  "left") \
            .withColumn("kwota_przel_pln",
                        when(
                            (col("oper.kwota_przel") != "0") & (col("oper.kwota_przel") != ""),
                            col("oper.kwota_przel") * col("kurs.kurs"))
                        .otherwise(col("oper.kwota") * col("kurs.kurs"))) \
            .withColumn("data_trans_new",
                        when((col("oper.data_trans").isNotNull()) & (col("oper.data_trans") < "2500-01-01"),
                             col("oper.data_trans")).otherwise("1900-01-02")) \
            .withColumn("waluta_rach", col("kart.waluta")) \
            .withColumnRenamed("rach", "rach_org") \
            .withColumn("rach", digits_only_udf(col("rach_org"))) \
            .withColumnRenamed("rach_kontr", "rach_kontr_org") \
            .withColumn("rach_kontr", digits_only_udf(col("rach_kontr_org"))) \
            .select("rach", "rach_org", "rach_kontr", "rach_kontr_org", col("data_trans_new").alias("data_trans"),
                    "oper.typ_trans", "oper.kwota",
                    "oper.rodz_trans", "oper.zleceniodawca", "oper.beneficjent", "oper.tytul", "oper.kraj_nadawcy",
                    "oper.kraj_odbiorcy", "oper.waluta", "waluta_rach", "oper.kwota_przel", "kwota_przel_pln",
                    "oper.id_banku",
                    "oper.rodz_rach", "oper.ref_trans", "oper.data_ksiegow", "oper.ref_bank_pelnom_1",
                    "oper.pesel_pelnom_1", "oper.dok_nr_pelnom_1", "oper.dane_pelnom_1", "oper.ref_bank_pelnom_2",
                    "oper.pesel_pelnom_2", "oper.dok_nr_pelnom_2", "oper.dane_pelnom_2", "oper.ref_bank_pelnom_3",
                    "oper.pesel_pelnom_3", "oper.dok_nr_pelnom_3", "oper.dane_pelnom_3", "oper.fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(operacje_transakcje.clean_path(partycja))

        app.add_partition(operacje_transakcje.clean_table, partycja, operacje_transakcje.clean_path(partycja))


def operacje_mas_transakcje_fn(fd_load_id, numer_ladowania):
    kartoteki_rachunek = TableInfo("kartoteki_rachunek")
    operacje_mas_transakcje = TableInfo("operacje_mas_transakcje")
    nbp_kursy = TableInfo("nbp_kursy")

    window = Window.partitionBy("kart.numer").orderBy(col("kart.data_pozyskania").desc(), col("kart.dat_danych").desc())
    distinct_kartoteki_rachunek = app.table(kartoteki_rachunek.stir_table, "kart") \
        .withColumn("newest_kart", fnc.row_number().over(window)) \
        .filter(col("newest_kart") == 1) \
        .drop("newest_kart")

    operacje_mas_transakcje_table = app.table(operacje_mas_transakcje.stir_table, "oper")

    partycje = operacje_mas_transakcje_table \
        .where((col("fd_load_id") == fd_load_id)) \
        .select(operacje_mas_transakcje_table.data_pozyskania) \
        .distinct() \
        .map(lambda row: row[0]) \
        .collect()

    for partycja in partycje:
        df = operacje_mas_transakcje_table \
            .where((col("data_pozyskania") == partycja) & (col("fd_load_id") == fd_load_id)) \
            .limit(100)

        df.withColumn("is_duplicate", duplikat(df.columns)) \
            .filter((col("is_duplicate") == 1) & (col("ref_trans") != "")) \
            .drop("is_duplicate") \
            .join(distinct_kartoteki_rachunek.alias("kart"), col("oper.rach") == col("kart.numer"), "left") \
            .join(app.table(nbp_kursy.clean_table, "kurs"),
                  (((col("oper.waluta") == col("kurs.waluta")) | (col("kart.waluta") == col("kurs.waluta")))
                   & (col("oper.data_trans") == col("kurs.data_waluty"))),
                  "left") \
            .withColumn("kwota_pln",
                        when(
                            (col("oper.kwota_przel") != "0") & (col("oper.kwota_przel") != ""),
                            col("oper.kwota_przel") * col("kurs.kurs"))
                        .otherwise(col("oper.kwota") * col("kurs.kurs"))) \
            .withColumn("data_trans_new",
                        when((col("oper.data_trans").isNotNull()) & (col("oper.data_trans") < "2500-01-01"),
                             col("oper.data_trans")).otherwise("1900-01-02")) \
            .withColumn("waluta_rach", col("kart.waluta")) \
            .withColumnRenamed("rach", "rach_org") \
            .withColumn("rach", digits_only_udf(col("rach_org"))) \
            .withColumnRenamed("rach_wirt", "rach_wirt_org") \
            .withColumn("rach_wirt", digits_only_udf(col("rach_wirt_org"))) \
            .withColumnRenamed("rach_kontr", "rach_kontr_org") \
            .withColumn("rach_kontr", digits_only_udf(col("rach_kontr_org"))) \
            .select("rach", "rach_org", "rach_wirt", "rach_wirt_org", "rach_kontr", "rach_kontr_org",
                    col("data_trans_new").alias("data_trans"),
                    "oper.typ_trans", "oper.kwota", "oper.rodz_trans", "oper.zleceniodawca", "oper.beneficjent",
                    "oper.tytul", "oper.kraj_nadawcy",
                    "oper.kraj_odbiorcy", "oper.waluta", "waluta_rach", "oper.kwota_przel", "oper.id_banku",
                    "oper.rodz_rach", "oper.ref_trans", "oper.data_ksiegow",
                    "oper.ref_bank_pelnom_1", "oper.pesel_pelnom_1", "oper.dok_nr_pelnom_1", "oper.dane_pelnom_1",
                    "oper.ref_bank_pelnom_2",
                    "oper.pesel_pelnom_2", "oper.dok_nr_pelnom_2", "oper.dane_pelnom_2", "oper.ref_bank_pelnom_3",
                    "oper.pesel_pelnom_3",
                    "oper.dok_nr_pelnom_3", "oper.dane_pelnom_3", "oper.fd_load_id") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(operacje_mas_transakcje.clean_path(partycja))

        app.add_partition(operacje_mas_transakcje.clean_table, partycja, operacje_mas_transakcje.clean_path(partycja))


def nbp_kursy_fn(data_od, data_do):
    nbp_kursy = TableInfo("nbp_kursy")

    nbp_tmp = app.table("fd_stage.nbp_kursy_tabela_b") \
        .withColumn("fd_data", col("fd_data").cast("STRING")) \
        .unionAll(app.sql("""
            SELECT 'polski zloty' AS nazwa_waluty, 1 AS przelicznik, 'PLN' AS kod_waluty, '1' AS kurs_sredni, 
                   '2020-01-01' AS fd_data""")) \
        .coalesce(4) \
        .persist()

    kursy_nbbp = nbp_tmp.where((nbp_tmp.fd_data >= data_od) & (nbp_tmp.fd_data < data_do))

    if not kursy_nbbp.rdd.isEmpty():
        def to_date(s):
            return datetime.strptime(s, "%Y-%m-%d")

        def date_as_string(d):
            return datetime.strftime(d, "%Y-%m-%d")

        def dni_w_roku():
            min_date, max_date = map(to_date, kursy_nbbp.agg(fnc.min("fd_data"), fnc.max("fd_data")).collect()[0])
            delta = max_date - min_date
            data = [Row(dzien_roku=date_as_string(min_date + timedelta(days=i))) for i in range(delta.days + 1)]
            return app.to_df(data)

        base = nbp_tmp.select("kod_waluty") \
            .join(dni_w_roku()) \
            .coalesce(100) \
            .distinct() \
            .alias("w") \
            .join(kursy_nbbp.alias("k"),
                  (col("w.dzien_roku") == col("k.fd_data")) & (col("w.kod_waluty") == col("k.kod_waluty")),
                  "left") \
            .orderBy("w.kod_waluty", "w.dzien_roku") \
            .select("w.dzien_roku", "k.nazwa_waluty", "k.przelicznik", "w.kod_waluty", "k.kurs_sredni") \
            .persist()

        uzupelnione = base.where(col("kurs_sredni").isNotNull())

        uzupelniane = base.where(col("kurs_sredni").isNull()).alias("u") \
            .join(nbp_tmp.alias("k"),
                  col("k.kurs_sredni").isNotNull() & (col("u.kod_waluty") == col("k.kod_waluty")) & (
                          col("u.dzien_roku") >= col("k.fd_data")),
                  "left") \
            .orderBy("u.kod_waluty", "u.dzien_roku") \
            .select("u.dzien_roku", "k.nazwa_waluty", "k.przelicznik", "u.kod_waluty", "k.kurs_sredni")

        uzupelnione.unionAll(uzupelniane) \
            .withColumnRenamed("dzien_roku", "data_waluty") \
            .withColumnRenamed("kod_waluty", "waluta") \
            .withColumnRenamed("kurs_sredni", "kurs") \
            .withColumnRenamed("przelicznik", "liczba_jednostek") \
            .orderBy("waluta", "data_waluty") \
            .repartition(1) \
            .write \
            .mode("append") \
            .parquet(nbp_kursy.clean_path())


def defragment_table(table_name, table_path, max_partitions):
    if app.table(table_name).rdd.getNumPartitions() > max_partitions:
        def stringify(s):
            return s if isinstance(s, (str, unicode)) else str(s)

        print("Redukuje liczbe partycji tabeli {0}".format(table_name))
        tmp_path = table_path + ".defrag"
        app.delete_file(tmp_path)
        app.table(table_name) \
            .coalesce(min(max_partitions, 4)) \
            .map(lambda row: "\t".join(map(stringify, row))) \
            .saveAsTextFile(tmp_path)
        app.move_file(tmp_path, table_path)
        print("Liczba partycji tabeli {0} zostala zredukowana".format(table_name))


class Ladowanie:
    def __init__(self, nazwa, funkcja, *tabele_zrodlowe):
        self.nazwa = nazwa
        self.funkcja = funkcja
        self.tabele_zrodlowe = list(tabele_zrodlowe)


def main():
    ladowania = [
        Ladowanie("blokady", blokady_fn, "stir_blokady"),
        Ladowanie("kartoteki_osoba", kartoteki_osoba_fn, "stir_kartoteki_osoba"),
        Ladowanie("kartoteki_podmiot", kartoteki_podmiot_fn, "stir_kartoteki_podmiot"),
        Ladowanie("kartoteki_rachunek", kartoteki_rachunek_fn, "stir_kartoteki_rachunek"),
        Ladowanie(
            "kartoteki_relacje_osoba_rachunek",
            kartoteki_relacje_osoba_rachunek_fn,
            "stir_kartoteki_relacje_osoba_rachunek"),
        Ladowanie("lokaty", lokaty_fn, "stir_lokaty"),
        Ladowanie("operacje_rachunki", operacje_rachunki_fn, "stir_operacje_rachunki"),
        Ladowanie("rach_wirt", rach_wirt_fn, "stir_rach_wirt"),
        Ladowanie("relacje_vat_inicjalne", relacje_vat_inicjalne_fn, "stir_relacje_vat_inicjalne"),
        Ladowanie("risk_score", risk_score_fn, "stir_risk_score"),
        Ladowanie("risk_score_rachunki", risk_score_rachunki_fn, "stir_risk_score_rachunki"),
        Ladowanie("risk_score_czynniki", risk_score_czynniki_fn, "stir_risk_score"),
        Ladowanie("operacje_transakcje", operacje_transakcje_fn, "stir_operacje_transakcje"),
        Ladowanie("operacje_mas_transakcje", operacje_mas_transakcje_fn, "stir_operacje_mas_transakcje"),
        Ladowanie(
            "pelnom_z_transakcji", pelnom_z_transakcji_fn, "stir_operacje_transakcje", "stir_operacje_mas_transakcje")
    ]

    proces = Proces(app.sql, app.table)
    proces.create_tables()
    (numer_ladowania, status, przyrost_od, przyrost_do) = proces.proces_do_zaladowania()

    paczki_do_zaladowania = proces.paczki_do_zaladowania(numer_ladowania, przyrost_od, przyrost_do)

    proces.dodaj_log(status, numer_ladowania, przyrost_od, przyrost_do)

    def log(msg, *args):
        print((datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f ") + " " + msg).format(*args))

    czy_zaladowano_kursy = proces.czy_zaladowano_kursy(numer_ladowania)
    if not czy_zaladowano_kursy:
        log("Laduje nbp_kursy od: {0} do: {1}", przyrost_od, przyrost_do)
        nbp_kursy_fn(przyrost_od, przyrost_do)
        proces.dodaj_paczke("nbp_kursy", "{0}-{1}".format(przyrost_od, przyrost_do), numer_ladowania)

    for ladowanie in ladowania:
        log("Rozpoczynam ladowanie tabeli {0}", ladowanie.nazwa)
        log("Zrodlowe tabele dla {0}: {1}", ladowanie.nazwa, ",".join(ladowanie.tabele_zrodlowe))
        paczki_dla_zrodel = [paczki_do_zaladowania.get(t, []) for t in ladowanie.tabele_zrodlowe]
        paczki_dla_tabeli = [item for zagniezdzona in paczki_dla_zrodel for item in zagniezdzona]
        log("{0} paczek do zaladowania dla tabeli: {1}", len(paczki_dla_tabeli), ladowanie.nazwa)

        n = len(paczki_dla_tabeli)
        for i, fd_load_id in enumerate(paczki_dla_tabeli):
            log("Ladowanie paczki {0}/{1} ({2}) dla tabeli: {3}", i + 1, n, fd_load_id, ladowanie.nazwa)
            ladowanie.funkcja(fd_load_id, numer_ladowania)
            proces.dodaj_paczke(ladowanie.nazwa, fd_load_id, numer_ladowania)

    log("Ladowanie numer: {0} zakonczone poprawnie", numer_ladowania)
    proces.dodaj_log(proces.status_koniec, numer_ladowania, przyrost_od, przyrost_do)

    proces.czyszczenie()


main()


