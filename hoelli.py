import random
import socket
import time
import urllib.request


def get_offset(px_cnt=0):
    # load offset
    url = 'http://hoellipixelflut.de/xy/?report={px_cnt}'.format(px_cnt=px_cnt)
    offset = urllib.request.urlopen(url).read()

    x, y = offset.decode().split()
    print('New offset: ', x, y)
    return int(x), int(y)


def get_img():
    lines = urllib.request.urlopen(
        'http://hoellipixelflut.de/hoelli.csv').read()
    lines = lines.decode('utf-8').split('\n')[:-1]

    img = []
    for line in lines:
        img.append(line.replace(' ', '').split(','))

    h = len(img)
    w = len(img[0])

    return img, w, h


def main():
    DT_OFFSET = 20.0
    DT_IMG = 60.0
    N_SOCKS = 16

    # connect
    sockets = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)
               for _ in range(N_SOCKS)]
    for sock in sockets:
        sock.connect(('151.217.111.34', 1234))

    dx, dy = get_offset()
    print(dx, dy)

    img, w, h = get_img()
    print(w, h)

    image_list = []
    for x in random.shuffle(range(w)):
        for y in random.shuffle(range(h)):
            rgb = img[y][x]

            if rgb == '000000':
                continue

            cmd = 'PX {xx} {yy} {rgb}\n'.format(xx=x + dx, yy=y + dy, rgb=rgb).encode()
            image_list.append(cmd)


    print('Start...')
    time0 = 0
    time1 = 0
    i_sock = 0
    px_cnt = 0
    i = 0
    image_list_index = 0
    while True:

        cmd = image_list[i]
        sockets[i_sock].send(cmd)
        i = i + 1 % len(image_list)

        px_cnt += 1
        i_sock = (i_sock + 1) % N_SOCKS

#        if i % 1024 == 0:
#            if time.time() - time0 > DT_OFFSET:
#                dx, dy = get_offset(px_cnt)
#               time0 = time.time()
#                px_cnt = 0

#            if time.time() - time1 > DT_IMG:
#                print('Update Image')
#                img, w, h = get_img()
#                time1 = time.time()
#        i += 1


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception:
            print()
