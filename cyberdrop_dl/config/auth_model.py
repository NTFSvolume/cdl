from pydantic import BaseModel

from cyberdrop_dl.config._common import ConfigFile
from cyberdrop_dl.models import AliasModel


class CoomerAuth(BaseModel):
    session: str = ""


class ImgurAuth(AliasModel):
    client_id: str = ""


class MegaNzAuth(AliasModel):
    email: str = ""
    password: str = ""


class JDownloaderAuth(AliasModel):
    username: str = ""
    password: str = ""
    device: str = ""


class KemonoAuth(AliasModel):
    session: str = ""


class GoFileAuth(AliasModel):
    api_key: str = ""


class PixeldrainAuth(AliasModel):
    api_key: str = ""


class RealDebridAuth(AliasModel):
    api_key: str = ""


class AuthSettings(ConfigFile):
    coomer: CoomerAuth = CoomerAuth()
    gofile: GoFileAuth = GoFileAuth()
    imgur: ImgurAuth = ImgurAuth()
    jdownloader: JDownloaderAuth = JDownloaderAuth()
    kemono: KemonoAuth = KemonoAuth()
    meganz: MegaNzAuth = MegaNzAuth()
    pixeldrain: PixeldrainAuth = PixeldrainAuth()
    realdebrid: RealDebridAuth = RealDebridAuth()
