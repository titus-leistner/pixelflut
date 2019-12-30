import random
import socket
import urllib.request
import sys
import hashlib
import selectors
import time
import threading

TRANSPARENT = '000000'


def version_hash():
    """
    Compute a short hash of the code file, for debugging purposes
    """
    with open(__file__, 'r') as f:
        hash_md5 = hashlib.md5(f.read().encode())
    return hash_md5.hexdigest()[:16]


def call_api(px_cnt=0, ver=''):
    """
    Call command and control API

    :param px_cnt: number of previously sendt pixels
    :type px_cnt: int

    :param ver: version to be sendt to the server
    :type ver: str

    :returns: (dx, dy, url, hostname, port, mode)
    """
    # get server api url from command line argument
    api_url = 'http://hoellipixelflut.de/client-api/ipv4/'
    if len(sys.argv) > 2:
        api_url = sys.argv[2]

    # request commands and report
    api_url = '{url}?pxc={px_cnt}&ver={ver}'.format(
        url=api_url, px_cnt=px_cnt, ver=ver)
    response = urllib.request.urlopen(api_url).read().decode()

    if 'Error' in response:
        raise Exception(response)

    # unpack commands
    dx, dy, url, hostname, port, mode = response.split()

    dx = int(dx)
    dy = int(dy)
    hostname = str(hostname)
    port = int(port)
    mode = str(mode)

    return dx, dy, url, hostname, port, mode


def load_img(url):
    """
    Load csv-encoded image from a given url

    :param url: the url
    :type url: str

    :returns: the image as nested list
    """
    print('Retrieving image...', end='', flush=True)

    lines = urllib.request.urlopen(url).read()
    lines = lines.decode('utf-8').split('\n')[:-1]

    img = []
    for line in lines:
        img.append(line.replace(' ', '').split(','))

    print(' Done. New image dimensions:', len(img[0]), len(img))

    return img


def get_cmds(dx, dy, img):
    """
    Compute the command string for the shuffeled image pixels

    :param dx: x-offset
    :type dx: int

    :param dy: y-offset
    :type dy: int

    :param img: the image as nested lists of rgb-values
    :type img: list(list(str))

    :returns: (command string, number of pixels)
    """
    print('Updating command string...', end='', flush=True)
    h = len(img)
    w = len(img[0])

    cmds = []
    for y in range(h):
        for x in range(w):
            rgb = img[y][x]

            # ignore transparent color
            if rgb == TRANSPARENT:
                continue

            # validate rgb value is hex and of right length
            int(rgb, 16)
            if len(rgb) != 6 and len(rgb) != 8:
                raise ValueError('RGB(A) Value has wrong length')
            cmds.append('PX {xx} {yy} {rgb}\n'.format(
                xx=x+dx, yy=y+dy, rgb=rgb).encode())

    print(' Done.')

    return cmds


class Sender():
    def __init__(self, max_socks=16):
        self.max_socks = max_socks
        self.sel = selectors.DefaultSelector()
        self.buf = {}
        self.px_cnt = 0.0
        self.cmd_str = {}

    def get_px_cnt(self):
        px_cnt = self.px_cnt
        self.px_cnt = 0.0
        return int(px_cnt)

    def set_cmd_list(self, cmds):
        for sock in self.sel.get_map().values():
            random.shuffle(cmds)
            self.cmd_str[sock] = b''.join(cmds)
        self.px_per_str = len(cmds)

    def connect(self, hostname, port):
        """
        Connect with maximum number of sockets to the pixel wall

        :param hostname: hostname of the pixel wall
        :type hostname: str

        :param port: port of the pixel wall
        :type port: int
        """
        # connect
        print('Connecting to the Pixelflut wall at {hn}, {port}'.format(
            hn=hostname, port=port), end='', flush=True)

        for i in range(self.max_socks):
            try:
                sock = socket.create_connection((hostname, port))
            except ConnectionRefusedError:
                break

            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2**18)
            self.sel.register(sock, selectors.EVENT_WRITE, self.send)
            sock.setblocking(0)

        if len(self.sel.get_map()) < 1:
            raise ConnectionRefusedError('Could not connect with any socket.')

        print(' Connected with {} sockets.'.format(len(self.sel.get_map())))

    def disconnect(self):
        """
        Disconnect sockets
        """
        for sock in self.sel.get_map().values():
            self.sel.unregister(sock)
            sock.close()

    def send(self, sock):
        """
        Send data to server

        :param sock: the socket
        :type sock: socket.socket
        """
        data = self.buf.get(sock, b'')
        sent = sock.send(data)
        self.buf[sock] = data[sent:]
        if len(data) == 0:
            self.buf[sock] = self.cmd_str.get(sock, b'')

        self.px_cnt += float(sent) / len(self.cmd_str) * self.px_per_str

    def send_idle(self):
        """
        Fire idle sockets
        """
        for k, v in self.sel.select():
            self.send(k.fileobj)


class Loops:
    def __init__(self):
        # initialize
        self.ver = version_hash()
        resp = call_api(0, self.ver)
        self.dx, self.dy, self.url, self.hostname, self.port, self.mode = resp
        self.img = load_img(self.url)

        # connect to wall
        max_socks = 128
        if len(sys.argv) > 1:
            max_socks = int(sys.argv[1])
        self.sender = Sender(max_socks)

        self.sender.connect(self.hostname, self.port)

        # precompute wall commands
        self.sender.set_cmd_list(get_cmds(self.dx, self.dy, self.img))

    def sending_loop(self):
        print('Let\'s Hölli...')

        while True:
            self.sender.send_idle()

    def api_loop(self):
        while True:
            # call API and update stuff, if necessary
            ndx, ndy, nurl, nhostname, nport, nmode = call_api(
                self.sender.get_px_cnt(), self.ver)

            if nurl != self.url:
                self.url = nurl
                self.img = load_img(self.url)
                self.sender.set_cmd_list(get_cmds(self.dx, self.dy, self.img))

            if ndx != self.dx or ndy != self.dy:
                self.dx, self.dy = ndx, ndy
                self.sender.set_cmd_list(get_cmds(self.dx, self.dy, self.img))

            if nhostname != self.hostname or nport != self.port:
                self.hostname, self.port = nhostname, nport
                self.sender.disconnect()
                self.sender.connect(self.hostname, self.port)
            time.sleep(10.0)


def main():
    print('USAGE: python3 hoelli.py [MAX_SOCKETS] [API_URL]')
    ver = version_hash()
    print('VERSION: {ver}'.format(ver=ver))
    loops = Loops()

    api_thread = threading.Thread(target=loops.api_loop)
    api_thread.start()

    loops.sending_loop()


if __name__ == '__main__':
    main()
    while True:
        try:
            main()
        except Exception as e:
            # catch all exceptions and restart ;)
            print('An exception encountered: ',
                  type(e),  e, ' Restarting...')
        time.sleep(10.0)
