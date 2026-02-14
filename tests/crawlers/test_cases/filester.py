DOMAIN = "filester"
TEST_CASES = [
    (
        "https://filester.me/d/Y9Vkbpq",
        [
            {
                "url": "re:https://cache1.filester.me/d/",
                "filename": "lilcanadiangirl_Stranger-in-the-Theatre.mp4",
                "referer": "https://filester.me/d/Y9Vkbpq",
                "album_id": None,
                "datetime": 1771027200,
            }
        ],
    ),
    (
        "https://filester.me/f/c3bf3e1da9982845",
        [
            {
                "url": "re:https://cache1.filester.me/d/",
                "download_folder": "re:mirror_post-410_1771095844 (Filester)",
                "album_id": "c3bf3e1da9982845",
            }
        ],
        53,
    ),
]
