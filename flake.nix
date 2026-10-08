{
  description = "Logos zcash_wallet_core_module: the Zcash wallet engine (librustzcash, SQLCipher, sync over Tor).";

  inputs = {
    logos-module-builder.url = "github:logos-co/logos-module-builder";
  };

  outputs = inputs@{ self, logos-module-builder, ... }:
    let
      nixpkgs = logos-module-builder.inputs.nixpkgs;
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems f;
      # Sapling proving parameters ship beside the plugin, fetched at build time with
      # pinned hashes, so a send never downloads them outside the proxy.
      saplingParams = system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
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
    in
    {
      packages = forAllSystems (system:
        (logos-module-builder.lib.mkLogosModule {
          src = ./.;
          configFile = ./metadata.json;
          flakeInputs = inputs;
          externalLibInputs.zcash_sapling_params = saplingParams system;
        }).packages.${system});
    };
}
