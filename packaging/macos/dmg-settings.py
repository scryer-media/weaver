# dmgbuild settings for the Weaver disk image.
#
# dmgbuild execs this file into a plain dict, so there is no `__file__` here and
# every path has to arrive as a `-D` define from build-dmg.sh.
import os

app_path = defines["app"]  # noqa: F821 - injected by dmgbuild
assets = defines["assets"]  # noqa: F821 - injected by dmgbuild
app_name = os.path.basename(app_path)

# UDZO is the compressed read-only format Finder mounts without a helper.
format = "UDZO"  # noqa: A001 - dmgbuild reads this name

files = [app_path]
symlinks = {"Applications": "/Applications"}

# `icon` is the volume icon. dmgbuild refuses to accept it together with
# `badge_icon`, so the bundle icon doubles as the mounted volume's.
icon = os.path.join(assets, "weaver.icns")
background = os.path.join(assets, "dmg-background.tiff")

window_rect = ((200, 120), (600, 400))
default_view = "icon-view"
show_status_bar = False
show_tab_view = False
show_toolbar = False
show_pathbar = False
show_sidebar = False

icon_size = 128
text_size = 12

# The background art draws an arrow between exactly these two positions, so
# moving one without redrawing the background leaves the arrow pointing at
# nothing. Positions are icon centers. The filename labels stay visible, and
# Finder always paints them black over a custom background picture (dark mode
# included, no setting to recolor them) — which is why the background art is
# light-themed.
icon_locations = {
    app_name: (140, 210),
    "Applications": (460, 210),
}
