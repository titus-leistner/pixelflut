import random
import socket
import time
import urllib.request
import sys

DT = 10.0
MAX_SOCKS = 16


def call_api(px_cnt=0):
    # load offset
    url = 'http://hoellipixelflut.de/client-api/?report={px_cnt}'.format(
        px_cnt=px_cnt)
    response = urllib.request.urlopen(url).read()

    x, y, url = response.decode().split()

    x = int(x)
    y = int(y)
    url = 'http://hoellipixelflut.de/images/' + url

    return x, y, url


def load_img(url):
    print('Retrieving image...', end='', flush=True)

    lines = urllib.request.urlopen(url).read()
    lines = lines.decode('utf-8').split('\n')[:-1]

    img = []
    for line in lines:
        img.append(line.replace(' ', '').split(','))

    print(' Done. New image dimensions:', len(img[0]), len(img))

    return img


def get_cmd_str(dx, dy, img):
    print('Updating command list...', end='', flush=True)
    h = len(img)
    w = len(img[0])

    cmds = []
    for y in range(h):
        for x in range(w):
            rgb = img[y][x]
            if rgb == '000000':
                continue
            int(rgb, 16)
            if len(rgb) != 6:
                raise ValueError()
            cmds.append('PX {xx} {yy} {rgb}\n'.format(
                xx=x+dx, yy=y+dy, rgb=rgb).encode())

    random.shuffle(cmds)
    n_px = len(cmds)
    cmd_str = b''.join(cmds)
    print(' Done.')

    return cmd_str, n_px


def main():
    # connect
    print('Connecting to the C3 Pixel Flut wall...', end='', flush=True)
    sockets = []
    for _ in range(MAX_SOCKS):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('151.217.111.34', 1234))
        except ConnectionRefusedError:
            break
        sockets.append(sock)

    if len(sockets) < 1:
        raise ConnectionRefusedError('Could not connect with any socket.')

    print(' Connected with {} sockets.'.format(len(sockets)))

    dx, dy, url = call_api()
    img = load_img(url)

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
            ndx, ndy, nurl = call_api(px_cnt)

            if nurl != url:
                url = nurl
                img = load_img(url)
                cmd_str, n_px = get_cmd_str(dx, dy, img)

            if ndx != dx or ndy != dy:
                dx, dy = ndx, ndy
                cmd_str, n_px = get_cmd_str(dx, dy, img)

            time0 = time.time()
            px_cnt = 0


if __name__ == '__main__':
    print('USAGE: python3 hoelli.py [MAX_SOCKETS]')
    if len(sys.argv) > 1:
        MAX_SOCKS = int(sys.argv[1])

    while True:
        try:
            main()
        except Exception as e:
            print('An exception encountered: ',
                  type(e),  e, ' . Restarting...')
