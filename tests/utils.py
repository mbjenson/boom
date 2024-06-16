import collections.abc as collections
import os
import traceback
from copy import deepcopy
from pathlib import Path
from typing import Mapping, Optional

import numpy as np
import pymongo
import yaml
from motor.motor_asyncio import AsyncIOMotorClient
from numba import jit
from pymongo.errors import BulkWriteError

# Cache loading of environment
_cache = {}


def recursive_update(d, u):
    # Based on https://stackoverflow.com/a/3233356/214686
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = recursive_update(d.get(k, {}) or {}, v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def relative_to(path, root):
    p = Path(path)
    try:
        return p.relative_to(root)
    except ValueError:
        return p


class Config(dict):
    """To simplify access, the configuration allows fetching nested
    keys separated by a period `.`, e.g.:

    >>> cfg['app.db']

    is equivalent to

    >>> cfg['app']['db']

    """

    def __init__(self, config_files=None):
        dict.__init__(self)
        if config_files is not None:
            cwd = os.getcwd()
            config_names = [relative_to(c, cwd) for c in config_files]
            print(f"  Config files: {config_names[0]}")
            for f in config_names[1:]:
                print(f"                {f}")
            self["config_files"] = config_files
            for f in config_files:
                self.update_from(f)

    def update_from(self, filename):
        """Update configuration from YAML file"""
        if os.path.isfile(filename):
            more_cfg = yaml.full_load(open(filename))
            recursive_update(self, more_cfg)

    def __getitem__(self, key):
        keys = key.split(".")

        val = self
        for key in keys:
            if isinstance(val, dict):
                val = dict.__getitem__(val, key)
            else:
                raise KeyError(key)

        return val

    def get(self, key, default=None, /):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def show(self):
        """Print configuration"""
        print()
        print("=" * 78)
        print("Configuration")
        for key in self:
            print("-" * 78)
            print(key)

            if isinstance(self[key], dict):
                for key, val in self[key].items():
                    print("  ", key.ljust(30), val)

        print("=" * 78)


def load_config(config_files=["config.yaml"]):
    """
    Load config and secrets
    """
    if not _cache:
        missing = [cfg for cfg in config_files if not os.path.isfile(cfg)]
        if missing:
            print(f'Missing config files: {", ".join(missing)}; continuing.')
        if "config.yaml" in missing:
            print(
                "Warning: You are running on the default configuration. To configure your system, "
                "please copy `config.defaults.yaml` to `config.yaml` and modify it as you see fit."
            )

        all_configs = [
            Path("config.defaults.yaml"),
        ] + config_files
        all_configs = [cfg for cfg in all_configs if os.path.isfile(cfg)]
        all_configs = [os.path.abspath(Path(c).absolute()) for c in all_configs]

        cfg = Config(all_configs)
        _cache.update({"cfg": cfg})

    return _cache["cfg"]


class Mongo:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 27017,
        replica_set: Optional[str] = None,
        username: str = None,
        password: str = None,
        db: str = None,
        srv: bool = False,
        verbose=0,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.replica_set = replica_set

        if srv is True:
            conn_string = "mongodb+srv://"
        else:
            conn_string = "mongodb://"

        if self.username is not None and self.password is not None:
            conn_string += f"{self.username}:{self.password}@"

        if srv is True:
            conn_string += f"{self.host}"
        else:
            conn_string += f"{self.host}:{self.port}"

        if db is not None:
            conn_string += f"/{db}"

        if self.replica_set is not None:
            conn_string += f"?replicaSet={self.replica_set}"
        else:
            conn_string += "?directConnection=true"

        self.client = pymongo.MongoClient(conn_string)
        self.db = self.client.get_database(db)

        self.verbose = verbose

    def insert_one(
        self, collection: str, document: dict, transaction: bool = False, **kwargs
    ):
        # note to future me: single-document operations in MongoDB are atomic
        # turn on transactions only if running a replica set
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_one(document, session=session)
            else:
                self.db[collection].insert_one(document)
        except Exception as e:
            if self.verbose:
                print(
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def insert_many(
        self, collection: str, documents: list, transaction: bool = False, **kwargs
    ):
        ordered = kwargs.get("ordered", False)
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_many(
                            documents, ordered=ordered, session=session
                        )
            else:
                self.db[collection].insert_many(documents, ordered=ordered)
        except BulkWriteError as bwe:
            if self.verbose:
                print(
                    f"Error inserting documents into collection {collection}: {str(bwe.details)}",
                )
                traceback.print_exc()
        except Exception as e:
            if self.verbose:
                print(
                    f"Error inserting documents into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def update_one(
        self,
        collection: str,
        filt: dict,
        update: dict,
        transaction: bool = False,
        **kwargs,
    ):
        upsert = kwargs.get("upsert", True)

        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].update_one(
                            filter=filt,
                            update=update,
                            upsert=upsert,
                            session=session,
                        )
            else:
                self.db[collection].update_one(
                    filter=filt, update=update, upsert=upsert
                )
        except Exception as e:
            if self.verbose:
                print(
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def delete_one(self, collection: str, document: dict, **kwargs):
        try:
            self.db[collection].delete_one(document)
        except Exception as e:
            print(
                f"Error deleting document from collection {collection}: {str(e)}",
            )
            traceback.print_exc()

    def close(self):
        try:
            self.client.close()
            return True
        except Exception as e:
            print(f"Error closing connection: {str(e)}")
            return False


async def get_mongo_async(config, verbose=True) -> AsyncIOMotorClient:
    """
    Initialize db if necessary: create the sole non-admin user
    """
    if config["database"].get("srv", False) is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    return AsyncIOMotorClient(conn_string)


def get_mongo(config, verbose=False) -> Mongo:
    return Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        srv=config["database"]["srv"],
        verbose=verbose,
    )


@jit
def great_circle_distance(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    """
        Distance between two points on the sphere
    :param ra1_deg:
    :param dec1_deg:
    :param ra2_deg:
    :param dec2_deg:
    :return: distance in degrees
    """
    # this is orders of magnitude faster than astropy.coordinates.Skycoord.separation
    DEGRA = np.pi / 180.0
    ra1, dec1, ra2, dec2 = (
        ra1_deg * DEGRA,
        dec1_deg * DEGRA,
        ra2_deg * DEGRA,
        dec2_deg * DEGRA,
    )
    delta_ra = np.abs(ra2 - ra1)
    distance = np.arctan2(
        np.sqrt(
            (np.cos(dec2) * np.sin(delta_ra)) ** 2
            + (
                np.cos(dec1) * np.sin(dec2)
                - np.sin(dec1) * np.cos(dec2) * np.cos(delta_ra)
            )
            ** 2
        ),
        np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(delta_ra),
    )

    return distance * 180.0 / np.pi


@jit
def in_ellipse(alpha, delta0, alpha1, delta01, d0, axis_ratio, PA0):
    """
        Check if a given point (alpha, delta0)
        is within an ellipse specified by
        center (alpha1, delta01), maj_ax (d0), axis ratio and positional angle
        All angles are in decimal degrees
        Adapted from q3c: https://github.com/segasai/q3c/blob/master/q3cube.c
    :param alpha:
    :param delta0:
    :param alpha1:
    :param delta01:
    :param d0:
    :param axis_ratio:
    :param PA0:
    :return:
    """
    DEGRA = np.pi / 180.0

    # convert degrees to radians
    d_alpha = (alpha1 - alpha) * DEGRA
    delta1 = delta01 * DEGRA
    delta = delta0 * DEGRA
    PA = PA0 * DEGRA
    d = d0 * DEGRA
    e = np.sqrt(1.0 - axis_ratio * axis_ratio)

    t1 = np.cos(d_alpha)
    t22 = np.sin(d_alpha)
    t3 = np.cos(delta1)
    t32 = np.sin(delta1)
    t6 = np.cos(delta)
    t26 = np.sin(delta)
    t9 = np.cos(d)
    t55 = np.sin(d)

    if (t3 * t6 * t1 + t32 * t26) < 0:
        return False

    t2 = t1 * t1

    t4 = t3 * t3
    t5 = t2 * t4

    t7 = t6 * t6
    t8 = t5 * t7

    t10 = t9 * t9
    t11 = t7 * t10
    t13 = np.cos(PA)
    t14 = t13 * t13
    t15 = t14 * t10
    t18 = t7 * t14
    t19 = t18 * t10

    t24 = np.sin(PA)

    t31 = t1 * t3

    t36 = 2.0 * t31 * t32 * t26 * t6
    t37 = t31 * t32
    t38 = t26 * t6
    t45 = t4 * t10

    t56 = t55 * t55
    t57 = t4 * t7
    t60 = (
        -t8
        + t5 * t11
        + 2.0 * t5 * t15
        - t5 * t19
        - 2.0 * t1 * t4 * t22 * t10 * t24 * t13 * t26
        - t36
        + 2.0 * t37 * t38 * t10
        - 2.0 * t37 * t38 * t15
        - t45 * t14
        - t45 * t2
        + 2.0 * t22 * t3 * t32 * t6 * t24 * t10 * t13
        - t56
        + t7
        - t11
        + t4
        - t57
        + t57 * t10
        + t19
        - t18 * t45
    )
    t61 = e * e
    t63 = t60 * t61 + t8 + t57 - t4 - t7 + t56 + t36

    return t63 > 0


def deg2hms(x):
    """Transform degrees to *hours:minutes:seconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [0, 360) to be written as a sexagesimal string.

    Returns
    -------
    out : str
        The input angle written as a sexagesimal string, in the
        form, hours:minutes:seconds.

    """
    if not 0.0 <= x < 360.0:
        raise ValueError("Bad RA value in degrees")
    _h = np.floor(x * 12.0 / 180.0)
    _m = np.floor((x * 12.0 / 180.0 - _h) * 60.0)
    _s = ((x * 12.0 / 180.0 - _h) * 60.0 - _m) * 60.0
    hms = f"{_h:02.0f}:{_m:02.0f}:{_s:07.4f}"
    return hms


def deg2dms(x):
    """Transform degrees to *degrees:arcminutes:arcseconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [-90, 90] to be converted.

    Returns
    -------
    out : str
        The input angle as a string, written as degrees:minutes:seconds.

    """
    if not -90.0 <= x <= 90.0:
        raise ValueError("Bad Dec value in degrees")
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = f"{_d:02.0f}:{_m:02.0f}:{_s:06.3f}"
    return dms


RGE = np.array(
    [
        [-0.054875539, -0.873437105, -0.483834992],
        [+0.494109454, -0.444829594, +0.746982249],
        [-0.867666136, -0.198076390, +0.455983795],
    ]
)


def radec2lb(ra, dec):
    """
        Convert $R.A.$ and $Decl.$ into Galactic coordinates $l$ and $b$
    ra [deg]
    dec [deg]

    return l [deg], b [deg]
    """
    ra_rad, dec_rad = np.deg2rad(ra), np.deg2rad(dec)
    u = np.array(
        [
            np.cos(ra_rad) * np.cos(dec_rad),
            np.sin(ra_rad) * np.cos(dec_rad),
            np.sin(dec_rad),
        ]
    )

    ug = np.dot(RGE, u)

    x, y, z = ug
    galactic_l = np.arctan2(y, x)
    galactic_b = np.arctan2(z, (x * x + y * y) ** 0.5)
    return np.rad2deg(galactic_l), np.rad2deg(galactic_b)


def alert_mongify(alert: Mapping):
    """
    Prepare a raw alert for ingestion into MongoDB:
      - add a placeholder for ML-based classifications
      - add coordinates for 2D spherical indexing and compute Galactic coordinates
      - extract the prv_candidates section
      - extract the fp_hists section (if it exists)

    :param alert:
    :return:
    """

    doc = dict(alert)

    # let mongo create a unique _id

    # placeholders for classifications
    doc["classifications"] = dict()

    # GeoJSON for 2D indexing
    doc["coordinates"] = {}
    _ra = doc["candidate"]["ra"]
    _dec = doc["candidate"]["dec"]
    # string format: H:M:S, D:M:S
    _radec_str = [deg2hms(_ra), deg2dms(_dec)]
    doc["coordinates"]["radec_str"] = _radec_str
    # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
    _radec_geojson = [_ra - 180.0, _dec]
    doc["coordinates"]["radec_geojson"] = {
        "type": "Point",
        "coordinates": _radec_geojson,
    }

    # Galactic coordinates l and b
    l, b = radec2lb(doc["candidate"]["ra"], doc["candidate"]["dec"])
    doc["coordinates"]["l"] = l
    doc["coordinates"]["b"] = b

    prv_candidates = deepcopy(doc["prv_candidates"])
    doc.pop("prv_candidates", None)
    if prv_candidates is None:
        prv_candidates = []

    # extract the fp_hists section, if it exists
    fp_hists = deepcopy(doc.get("fp_hists", None))
    doc.pop("fp_hists", None)
    if fp_hists is None:
        fp_hists = []
    else:
        # sort by jd
        fp_hists = sorted(fp_hists, key=lambda k: k["jd"])

    return doc, prv_candidates, fp_hists


def flux_to_mag(flux, fluxerr, zp):
    """Convert flux to magnitude and calculate SNR

    :param flux:
    :param fluxerr:
    :param zp:
    :param snr_threshold:
    :return:
    """
    # make sure its all numpy floats or nans
    values = np.array([flux, fluxerr, zp], dtype=np.float64)
    snr = values[0] / values[1]
    if values[0] > 0:
        mag = -2.5 * np.log10(abs(values[0])) + values[2]
    else:
        mag = np.nan
    magerr = 1.0857 * (values[1] / values[0])
    limmag3sig = -2.5 * np.log10(3 * values[1]) + values[2]
    limmag5sig = -2.5 * np.log10(5 * values[1]) + values[2]
    if np.isnan(snr):
        return {}
    mag_data = {
        "mag": mag,
        "magerr": magerr,
        "snr": snr,
        "limmag3sig": limmag3sig,
        "limmag5sig": limmag5sig,
    }
    # remove all NaNs fields
    mag_data = {k: v for k, v in mag_data.items() if not np.isnan(v)}
    return mag_data


def format_fp_hists(alert, fp_hists):
    if len(fp_hists) == 0:
        return []
    # sort by jd
    fp_hists = sorted(fp_hists, key=lambda x: x["jd"])

    # deduplicate by jd. We noticed in production that sometimes there are
    # multiple fp_hist entries with the same jd, which is not supposed to happen
    # and can affect our concurrency avoidance logic in update_fp_hists and take more space
    fp_hists = [
        fp_hist
        for i, fp_hist in enumerate(fp_hists)
        if i == 0 or fp_hist["jd"] != fp_hists[i - 1]["jd"]
    ]

    # add the "alert_mag" field to the new fp_hist
    # as well as alert_ra, alert_dec
    for i, fp in enumerate(fp_hists):
        fp_hists[i] = {
            **fp,
            **flux_to_mag(
                flux=fp.get("forcediffimflux", np.nan),
                fluxerr=fp.get("forcediffimfluxunc", np.nan),
                zp=fp["magzpsci"],
            ),
            "alert_mag": alert["candidate"]["magpsf"],
            "alert_ra": alert["candidate"]["ra"],
            "alert_dec": alert["candidate"]["dec"],
        }

    return fp_hists
