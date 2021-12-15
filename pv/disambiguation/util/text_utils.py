def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid
