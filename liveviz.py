import pygame
import Image
from pygame.locals import *
import sys

import Image

im = Image.open("/home/bresnaha/x.png")

def get_image():
    return im.getdata()


pygame.init()
window = pygame.display.set_mode((1440,900))
pygame.display.set_caption("Phantom Demo")
screen = pygame.display.get_surface()

y = 0
pg_img = pygame.image.load("/home/bresnaha/x.png")
while True:
    events = pygame.event.get()
    for event in events:
        if event.type == QUIT or event.type == KEYDOWN:
            sys.exit(0)
    screen.blit(pg_img, (0,0))
    pygame.display.flip()
    pygame.time.delay(int(10))
    for i in range(200):
        pg_img.set_at((i, y), (255, 25, 25, 25))
    y = y + 10

