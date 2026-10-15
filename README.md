# ChunkSave

A Fabric 1.20.1 mod that stops your game from running out of memory while you pre-generate huge worlds.

It's built for the monster worldgen mods like [JJThunder](https://modrinth.com/datapack/jjthunder-to-the-max) and [Big Globe](https://modrinth.com/mod/big-globe), used with a pre-generator like [Chunky](https://github.com/pop4959/Chunky). It costs almost nothing in performance.

## The problem

When you pre-generate a world, Minecraft often creates chunks faster than your drive can write them. Vanilla's answer is to keep all the unsaved chunks in RAM. If your drive can't keep up, the RAM fills up, and then everything crashes. Hours of generation, gone, because the disk was a bit slow.

## The fix

ChunkSave works like a traffic controller between world generation and your drive. It sets a hard limit on how many chunks can wait in memory. When the limit is hit, generation pauses until the drive catches up, and then carries on.

## Install

Put the `.jar` in your `mods` folder. You need [Fabric Loader](https://fabricmc.net/use/installer/) 0.15 or newer and the [Fabric API](https://modrinth.com/mod/fabric-api).

## Build

Install JDK 17, then run `gradle build` in the project folder. The mod ends up in `build/libs/`.

## License

MIT.
