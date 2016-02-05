
import luigi
import luigi.postgres
import json
import brd
import brd.service
import datetime


class DownLoadThingISBN(luigi.Task):
    filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filename)


class BulkLoadThingISBN(luigi.postgres.CopyToTable):
    filename = luigi.Parameter(default=brd.config.REF_DATA_DIR + '/thingISBN_10000_26-1-2016.csv')

    def requires(self):
        return DownLoadThingISBN(self.filename)

    host        = brd.config.DATABASE['host']
    database    = brd.config.DATABASE['database']
    user        = brd.config.DATABASE['user']
    password    = brd.config.DATABASE['password']
    table = 'staging.thingisbn'
    columns = [("WORK_UID", "TEXT"),
            ("ISBN_ORI", "TEXT"),
            ("ISBN10", "TEXT"),
            ("ISBN13", "TEXT"),
            ("LOAD_AUDIT_ID", "INT")]
    column_separator = "|"


class BaseManualComplete(luigi.Task):
    completed = False

    def complete(self):
        return self.completed

# TODO: this works with attribute completed, howveer these tasks get executed everytime
# (no output Target luigi can verify!... must use some kind of checkpoint on the table)
class LoadWorkRef(BaseManualComplete):
    def requires(self):
        return BulkLoadThingISBN()

    def run(self):
        brd.service.load_work_ref()
        self.completed = True


class LoadIsbnRef(BaseManualComplete):
    def requires(self):
        return BulkLoadThingISBN()

    def run(self):
        brd.service.load_isbn_ref()
        self.completed = True


class LoadWorkIsbnRef(BaseManualComplete):
    def requires(self):
        return [LoadWorkRef(), LoadIsbnRef()]

    def run(self):
        brd.service.load_work_isbn_ref()
        self.completed = True


# serve as entry point to load all work-reference
class LoadWorkSiteMappingRef(luigi.Task):
    site_logical_name = luigi.Parameter(default='librarything')

    def requires(self):
        return [LoadWorkIsbnRef()]

    def run(self):
        brd.service.load_work_site_mapping(self.site_logical_name)


class FetchWorkids(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget('/tmp/wids_%s.txt' % self.harvest_dts.strftime('%Y%m%d-%H%M%S'))

    def run(self):
        f = self.output().open('w')
        for wid in brd.service.fetch_work(self.spidername, harvested=False, nb_work=self.n_work):
            json.dump(wid, f)
            f.write('\n')
        f.close()


class HarvestReviews(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateParameter()

    file_path = brd.config.SCRAPED_OUTPUT_DIR + '/ReviewsOf%s_%s.csv'

    def requires(self):
        return FetchWorkids(self.spidername, self.n_work, self.harvest_dts)

    def output(self):
        return luigi.LocalTarget(self.file_path % (self.spidername,
                                                   self.harvest_dts.strftime('%Y%m%d-%H%M%S')))

    def run(self):
        f = self.input().open('r')
        content = parse_wids_file(f)
        f.close()

        fo = self.output().open('w')
        fo.write(str(content))
        fo.close()


class UpdateLastHarvestDate(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        return {'wids': FetchWorkids(self.spidername, self.n_work, self.harvest_dts),
                'harvest': HarvestReviews(self.spidername, self.n_work, self.harvest_dts)}

    def run(self):
        wids_file = self.input()['wids'].open('r')
        wids = parse_wids_file(wids_file, True)
        brd.service.update_harvest_date(self.spidername, self.havest_dts, wids)
        wids_file.close()


def parse_wids_file(wf, only_ids=False):
    list_wids = []
    for line in wf:
        jw = json.loads(line)
        if only_ids:
            list_wids.append(jw['work-ori-id'])
        else:
            list_wids.append(jw)
    return list_wids

