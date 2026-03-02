from cyberdrop_dl.models import AliasModel, Settings


class Coomer(AliasModel):
    session: str = ""


class Imgur(AliasModel):
    client_id: str = ""


class MegaNz(AliasModel):
    email: str = ""
    password: str = ""


class JDownloader(AliasModel):
    username: str = ""
    password: str = ""
    device: str = ""


class Kemono(AliasModel):
    session: str = ""


class GoFile(AliasModel):
    api_key: str = ""


class Pixeldrain(AliasModel):
    api_key: str = ""


class RealDebrid(AliasModel):
    api_key: str = ""


class AuthSettings(Settings):
    coomer: Coomer = Coomer()
    gofile: GoFile = GoFile()
    imgur: Imgur = Imgur()
    jdownloader: JDownloader = JDownloader()
    kemono: Kemono = Kemono()
    meganz: MegaNz = MegaNz()
    pixeldrain: Pixeldrain = Pixeldrain()
    realdebrid: RealDebrid = RealDebrid()
