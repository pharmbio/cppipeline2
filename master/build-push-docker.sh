docker build -t ghcr.io/pharmbio/cpp2_master:latest .
docker push ghcr.io/pharmbio/cpp2_master:latest

read -p "Do you want to push with \"stable\" tag also? [y|n]" -n 1 -r < /dev/tty
echo
if ! grep -qE "^[Yy]$" <<< "$REPLY"; then
    exit 1
fi

docker build -t ghcr.io/pharmbio/cpp2_master:stable .
docker push ghcr.io/pharmbio/cpp2_master:stable