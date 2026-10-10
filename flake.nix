{
  description = "Logos zcash_wallet_core_module: the Zcash wallet engine (librustzcash, SQLCipher, sync over Tor).";

  inputs = {
    logos-module-builder.url = "github:logos-co/logos-module-builder";
    # OPTIONAL in metadata.json: only its contract is consumed.
    zebrad_module = {
      url = "github:logos-co/logos-zebrad-module";
      inputs.logos-module-builder.follows = "logos-module-builder";
    };
  };

  outputs = inputs@{ self, logos-module-builder, ... }:
    let
      nixpkgs = logos-module-builder.inputs.nixpkgs;
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      # x86_64-windows is a cross build from x86_64-linux.
      targets = systems ++ [ "x86_64-windows" ];
      forAllSystems = f: nixpkgs.lib.genAttrs targets f;
      # Sapling proving parameters ship beside the plugin, fetched at build time with
      # pinned hashes, so a send never downloads them outside the proxy.
      saplingParams = system:
        # The parameter files are data; fetch them with the builder's own package set.
        let pkgs = nixpkgs.legacyPackages.${if system == "x86_64-windows" then "x86_64-linux" else system}; in
        pkgs.runCommand "zcash-sapling-params" {
          spend = pkgs.fetchurl {
            url = "https://download.z.cash/downloads/sapling-spend.params";
            hash = "sha256-jkj/0jq7Ol/ZxViSBPMtnDEoWgS3gJa6QKebdWd+/BM=";
          };
          output = pkgs.fetchurl {
            url = "https://download.z.cash/downloads/sapling-output.params";
            hash = "sha256-Lw67y7m7C8/+laOX5+uonCnrTd5hkcM524hXDj8/sOQ=";
          };
        } ''
          mkdir -p $out/lib
          cp $spend $out/lib/sapling-spend.params
          cp $output $out/lib/sapling-output.params
        '';
      # tools/regtest/chain.sh with what it runs on, for test harnesses such as the app's doctest.
      # ZEBRAD and LIGHTWALLETD stay the caller's: logos-zebra-nix builds both.
      # The same script and heights for Windows, run by Git Bash: curl and perl come from the
      # runner there, not from the store.
      regtestChainWindows =
        let pkgs = nixpkgs.legacyPackages.x86_64-linux; in
        pkgs.runCommand "zcash-regtest-chain-windows" { } ''
          mkdir -p $out/bin $out/share/regtest
          install -m 755 ${./tools/regtest/chain.sh} $out/share/regtest/chain.sh
          install -m 644 ${./tools/regtest/regtest.json} $out/share/regtest/regtest.json
          printf '#!/bin/bash\nexec "$(dirname "$0")/../share/regtest/chain.sh" "$@"\n' > $out/bin/regtest-chain
          chmod +x $out/bin/regtest-chain
        '';
      regtestChain = system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
        pkgs.runCommand "zcash-regtest-chain" { nativeBuildInputs = [ pkgs.makeWrapper ]; } ''
          mkdir -p $out/bin $out/share/regtest
          install -m 755 ${./tools/regtest/chain.sh} $out/share/regtest/chain.sh
          install -m 644 ${./tools/regtest/regtest.json} $out/share/regtest/regtest.json
          patchShebangs $out/share/regtest
          makeWrapper $out/share/regtest/chain.sh $out/bin/regtest-chain \
            --prefix PATH : ${nixpkgs.lib.makeBinPath [ pkgs.curl pkgs.perl ]}
        '';
    in
    {
      packages = forAllSystems (system:
        (logos-module-builder.lib.mkLogosModule {
          src = ./.;
          configFile = ./metadata.json;
          flakeInputs = inputs;
          externalLibInputs.zcash_sapling_params = saplingParams system;
        }).packages.${system}
        // { regtest-chain = if system == "x86_64-windows" then regtestChainWindows else regtestChain system; });
    };
}
