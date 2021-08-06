from node_scarper.unisvapv2_scarper import UniswapV2Scarper


def run():
    univ2_scarper = UniswapV2Scarper("0xebfb684dd2b01e698ca6c14f10e4f289934a54d6", 1627592400)
    univ2_scarper.scarp_data()


if __name__ == "__main__":
    run()
