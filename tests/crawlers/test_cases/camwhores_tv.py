DOMAIN = "camwhores_tv"
TEST_CASES = [
    (
        "https://www.camwhores.tv/videos/16553911/bellacle-ticket-show-1/",
        [
            {
                "url": "https://www.camwhores.tv/videos/16553911/bellacle-ticket-show-1/",
                "filename": "Bellacle Ticket Show 1 [16553911].mp4",
                "debrid_link": str,
                "original_filename": "16553911.mp4",
                "referer": "https://www.camwhores.tv/videos/16553911/bellacle-ticket-show-1/",
                "album_id": None,
                "uploaded_at": None,
                "download_folder": "re:Loose Files (Camwhores.tv)",
            }
        ],
        1,
    ),
    (
        "https://www.camwhores.tv/search/niemira/",
        [
            {
                "url": "ANY",
                "album_id": None,
                "uploaded_at": None,
                "download_folder": "re:niemira [search] (Camwhores.tv)",
            },
        ],
        range(22, 30),
    ),
]
