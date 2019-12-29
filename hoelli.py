import random
import socket
import time
import urllib.request
import sys
import hashlib

DT = 10.0
MAX_SOCKS = 128
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
    if len(sys.argv) > 1:
        api_url = sys.argv[1]

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


def get_cmd_str(dx, dy, img):
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

    random.shuffle(cmds)
    n_px = len(cmds)
    cmd_str = b''.join(cmds)
    print(' Done.')

    return cmd_str, n_px


def connect_wall(hostname, port):
    """
    Connect with maximum number of sockets to the pixel wall

    :param hostname: hostname of the pixel wall
    :type hostname: str

    :param port: port of the pixel wall
    :type port: int
    """
    # connect
    print('Connecting to the Pixel Flut wall at {hn}, {port}'.format(
        hn=hostname, port=port), end='', flush=True)
    sockets = []
    for _ in range(MAX_SOCKS):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((hostname, port))
        except ConnectionRefusedError:
            break
        sockets.append(sock)

    if len(sockets) < 1:
        raise ConnectionRefusedError('Could not connect with any socket.')

    print(' Connected with {} sockets.'.format(len(sockets)))
    return sockets


def main():
    print('USAGE: python3 hoelli.py [API_URL]')
    ver = version_hash()
    print('VERSION: {ver}'.format(ver=ver))

    # call API once
    dx, dy, url, hostname, port, mode = call_api(0, ver)
    img = load_img(url)

    # connect to wall
    sockets = connect_wall(hostname, port)

    # precompute wall commands
    cmd_str, n_px = get_cmd_str(dx, dy, img)

    print('Let\'s Hölli...')

    time0 = time.time()
    i_sock = 0
    px_cnt = 0

    while True:
        sockets[i_sock].send(cmd_str)
        px_cnt += n_px
        i_sock = (i_sock + 1) % len(sockets)

        if time.time() - time0 > DT:
            # each DT, call API and update stuff, if necessary
            ndx, ndy, nurl, nhostname, nport, nmode = call_api(px_cnt, ver)

            if nurl != url:
                url = nurl
                img = load_img(url)
                cmd_str, n_px = get_cmd_str(dx, dy, img)

            if ndx != dx or ndy != dy:
                dx, dy = ndx, ndy
                cmd_str, n_px = get_cmd_str(dx, dy, img)

            if nhostname != hostname or nport != port:
                for sock in sockets:
                    sock.close()
                hostname, port = nhostname, nport
                sockets = connect_wall(hostname, port)

            time0 = time.time()
            px_cnt = 0


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            # catch all exceptions and restart ;)
            print('An exception encountered: ',
                  type(e),  e, 'Restarting...')
        time.sleep(10.0)
