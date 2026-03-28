import datetime

from cyberdrop_dl.utils import webdav

XML = """<?xml version="1.0"?>
<d:multistatus
	xmlns:d="DAV:"
	xmlns:oc="http://owncloud.org/ns"
	xmlns:nc="http://nextcloud.org/ns">
	<d:response>
		<d:href>/public.php/dav/files/e5mYoDxSSGn2b</d:href>
		<d:propstat>
			<d:prop>
				<d:displayname>movie.mp4</d:displayname>
				<d:getcontenttype>video/mp4</d:getcontenttype>
				<d:resourcetype/>
				<d:getetag>&quot;ac8d5ef02ce089df735bf8c3813be492&quot;</d:getetag>
				<d:getcontentlength>422682383</d:getcontentlength>
				<d:getlastmodified>Fri, 27 Mar 2026 22:03:10 GMT</d:getlastmodified>
				<d:creationdate>1970-01-01T00:00:00+00:00</d:creationdate>
			</d:prop>
			<d:status>HTTP/1.1 200 OK</d:status>
		</d:propstat>
	</d:response>
</d:multistatus>
"""


def test_webdav_resp_parsing() -> None:
    result = tuple(webdav.parse_resp(XML))
    assert len(result) == 1
    node = result[0]
    assert node == webdav.Node(
        display_name="movie.mp4",
        content_type="video/mp4",
        resource_type="",
        etag="ac8d5ef02ce089df735bf8c3813be492",
        content_length=422682383,
        last_modified=datetime.datetime(2026, 3, 27, 22, 3, 10, tzinfo=datetime.UTC),
        creation_date=datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.UTC),
        href="/public.php/dav/files/e5mYoDxSSGn2b",
        status="HTTP/1.1 200 OK",
    )
