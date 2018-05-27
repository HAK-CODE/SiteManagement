import libtmux

server = libtmux.Server()
session = server.new_session("testingnew")
window = session.attached_window
window.attached_pane.send_keys("python main.py --siteId='a08394a2-9d25-4126-8adf-a13183f723ee'")

