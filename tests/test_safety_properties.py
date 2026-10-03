"""Generated URL identities and scope boundaries, independent of the parser."""

from urllib.parse import quote

from hypothesis import given, settings
from hypothesis import strategies as st

from mirror_url.utils import _relative_url_path, url_within_scope

ROOT = "https://example.com/root/"
component = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-; %é", min_size=1, max_size=35)
bounded = settings(max_examples=250, derandomize=True, deadline=None)


@bounded
@given(st.lists(component, min_size=1, max_size=5), st.text(alphabet="abc123&=", max_size=25))
def test_once_decoded_identity_ignores_query_and_fragment(parts, query):
    encoded = "/".join(quote(part, safe="") for part in parts)
    url = ROOT + encoded + "?" + query + "#ignored"
    assert _relative_url_path(url, ROOT) == "/".join(parts)
    assert url_within_scope(url, ROOT)
    assert _relative_url_path(url, "https://example.com/%72oot/") == "/".join(parts)


@bounded
@given(
    component,
    st.sampled_from(
        [
            "https://other.com/root/",
            "http://example.com/root/",
            "https://example.com/root-other/",
            "https://example.com:444/root/",
        ]
    ),
)
def test_origin_and_directory_boundaries_cannot_be_prefix_matches(name, foreign):
    assert not url_within_scope(foreign + quote(name, safe=""), ROOT)


@bounded
@given(
    st.sampled_from(["..", ".", "a\\b", "a\0b"]), st.integers(min_value=0, max_value=3), component
)
def test_repeated_encoding_cannot_hide_traversal(segment, encodings, name):
    for _ in range(encodings):
        segment = "".join("%{:02X}".format(byte) for byte in segment.encode())
    assert _relative_url_path(ROOT + segment + "/" + quote(name, safe=""), ROOT) is None


def test_invalid_url_and_nested_encoding_boundaries():
    assert _relative_url_path("http://[broken", ROOT) is None
    assert _relative_url_path(ROOT, ROOT) == ""
    assert _relative_url_path(ROOT.rstrip("/"), ROOT) == ""
    assert _relative_url_path(ROOT + "%252ename", ROOT) == "%2ename"
