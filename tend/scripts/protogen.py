import os.path
import subprocess
import tempfile
from urllib.request import urlretrieve

import tend

def main():
    bases = {
        'tendermint': 'https://raw.githubusercontent.com/tendermint/tendermint/main/proto/',
        'gogoproto': 'https://raw.githubusercontent.com/gogo/protobuf/master/'
    }
    proto_path = os.path.join(tempfile.mkdtemp(), 'proto')
    output_path = os.path.join(tend.__path__._path[0], 'pb')
    os.makedirs(output_path, exist_ok=True)
    proto_files = [
        'tendermint/abci/types.proto',
        'tendermint/crypto/keys.proto',
        'tendermint/crypto/proof.proto',
        'tendermint/types/types.proto',
        'tendermint/types/params.proto',
        'gogoproto/gogo.proto',
        'tendermint/version/types.proto',
        'tendermint/types/validator.proto'
    ]
    for filename in proto_files:
        for name, base in bases.items():
            if filename.startswith(name):
                url = f'{base}/{filename}'
                filename = os.path.join(proto_path, filename)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                urlretrieve(url, filename)
                break
        else:
            raise FileNotFoundError(f'Not found source for {filename}')
    ns_files = dict()
    for filename in proto_files:
        ns = tuple(filename.split(os.sep)[:-1])
        ns_files.setdefault(ns, []).append(filename)
    for ns, files in ns_files.items():
        if len(files) > 1:
            filename = os.path.join(proto_path, '_'.join(ns))
            with open(filename, 'w') as w:
                for filename_ in files:
                    with open(os.path.join(proto_path, filename_), 'r') as r:
                        w.write(r.read()+'\n\n')
        else:
            filename = os.path.join(proto_path, files[0])
        subprocess.run(['python', '-m',
                        'grpc_tools.protoc', '-I', proto_path,
                        f'--python_betterproto_out={output_path}', filename])


if __name__ == '__main__':
    main()