import sys

def build_mine(file, k, numThr, w, h, maxIt, skip):
    wh = w * h
    xa = -2.0
    xb = 1.0
    ya = -1.5
    yb = 1.5
    xd = xb - xa
    yd = yb - ya

    # each thread only calculates its own share of pixels
    for i in range(k, wh, numThr):
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
    out_dirname = argv[1]
    my_rank = int(argv[2])
    worker_count = int(argv[3])
    h = int(argv[4])
    w = h

    fptr = open("%s/out%d.data" % (out_dirname, my_rank), "w")
    build_mine(fptr, my_rank, worker_count, w, h, 256, 0)


if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
