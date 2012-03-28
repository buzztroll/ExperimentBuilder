import sys

def build_mine(file, k, numThr, w, h, maxIt):
    wh = w * h
    xa = -2.0
    xb = 1.0
    ya = -1.5
    yb = 1.5
    xd = xb - xa
    yd = yb - ya

    # each thread only calculates its own share of pixels
    for i in range(k, wh, numThr):
        file.write("CHECKPOINT:%d\n" % (i))
        kx = i % w
        ky = int(i / w)
        a = xa + xd * kx / (w - 1.0)
        b = ya + yd * ky / (h - 1.0)
        x = a
        y = b
        for kc in range(maxIt):
            x0 = x * x - y * y + a
            y = 2.0 * x * y + b
            x = x0
            if x * x + y * y > 4:
                # various color palettes can be created here
                red = (kc % 8) * 32
                green = (16 - kc % 16) * 16
                blue = (kc % 16) * 16

                out_line = "%d %d %d %d %d\n" % (kx, ky, red, green, blue)
                file.write(out_line)
                break

def main(argv=sys.argv):
    my_rank = int(argv[1])
    worker_count = int(argv[2])
    h = int(argv[3])
    last_checkpoint = int(argv[4])
    w = h

    if last_checkpoint != 0:
        my_rank = last_checkpoint
        
    build_mine(sys.stdout, my_rank, worker_count, w, h, 256)


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
