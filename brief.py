# Backward-compatible CLI entrypoint for the publisher intel job.

from publisher_intel import main


if __name__ == "__main__":
    # Keeping this file small means existing local commands still work even
    # though the actual implementation now lives in publisher_intel.py.
    main()
