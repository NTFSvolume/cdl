DOMAIN = "bunkr"
TEST_CASES = [
    (
        "https://bunkr.ax/v/rFicV4QnhSHBE",
        [
            {
                "url": r"re:1df93418-5063-4e1b-851e-9470cb8fc5c6\.mp4",
                "filename": "MysteriousProd.24.09.06.April.Olsen.Rebel.Rhyder.All.About.Fucking.720p.mp4",
                "referer": "https://bunkr.site/f/rFicV4QnhSHBE",
                "album_id": None,
                "uploaded_at": None,
            }
        ],
    ),
    (
        "https://get.bunkrr.su/file/41348624",
        [
            {
                "url": r"re:1df93418-5063-4e1b-851e-9470cb8fc5c6\.mp4",
                "filename": "MysteriousProd.24.09.06.April.Olsen.Rebel.Rhyder.All.About.Fucking.720p.mp4",
                "referer": "https://get.bunkrr.su/file/41348624",
                "album_id": None,
                "uploaded_at": None,
            }
        ],
    ),
    (
        "https://cdn9.bunkr.ru/24578-hd-kEMMY0JH.mp4",
        [
            {
                "url": r"re:24578-hd-kEMMY0JH.mp4",
                "filename": "24578-hd.mp4",
                "referer": "https://bunkr.site/f/24578-hd-kEMMY0JH.mp4",
            }
        ],
    ),
    (
        "https://bunkr.sk/f/summertimejames-pics--VocHZQ0K.rar",
        [
            {
                "url": "re:/summertimejames-pics--VocHZQ0K.rar?n=summertimejames(pics).rar",
                "filename": "summertimejames(pics).rar",
                "referer": "https://bunkr.site/f/summertimejames-pics--VocHZQ0K.rar",
            }
        ],
    ),
    (
        "https://kebab.bunkr.ru/summertimejames-pics--VocHZQ0K.rar",
        [
            {
                "url": "re:/summertimejames-pics--VocHZQ0K.rar?n=summertimejames-pics--VocHZQ0K.rar",
                "filename": "summertimejames-pics--VocHZQ0K.rar",
                "referer": "https://get.bunkrr.su",
            }
        ],
    ),
    (
        "https://burger.bunkr.ru/9861917.mp4-PTaiPNai-CaBcktkP.mp4",
        [
            {
                "url": "re:/9861917.mp4-PTaiPNai-CaBcktkP.mp4?n=9861917.mp4-PTaiPNai-CaBcktkP.mp4",
                "filename": "9861917.mp4-PTaiPNai-CaBcktkP.mp4",
                "referer": "https://get.bunkrr.su",
            }
        ],
    ),
    (
        # paginated
        "https://bunkr.cr/a/5aZU25Cb",
        [
            {
                "url": "ANY",
                "download_folder": r"re:abbywinters - Elin & Maddie (Girl - Girl Extra Large)",
                "album_id": "5aZU25Cb",
            }
        ],
        257,
    ),
    (
        "https://bunkr.cr/a/TQAgjP8m",
        [
            {
                "url": "ANY",
                "download_folder": r"re:NerdballerTV - Videos (2018-2023) [Complete]",
                "album_id": "TQAgjP8m",
                "uploaded_at": int,
            }
        ],
        220,
    ),
    (
        # .org domain redirect to a different domain and discards query params
        # This test is to make sure CDL does not get stuck in an infinite loop while doing album pagination
        "https://bunkrrr.org/a/n12rHpzB",
        [],
        141,
    ),
    ("https://bunkr.ws/a/z5Xt6NqH", [], 3),
    ("https://bunkr.ws/a/aJHkJf3L", [], 172),
]
