#if !NETSTANDARD2_0
using System.Runtime.Intrinsics.X86;
#endif

#if NET6_0_OR_GREATER
using System.Runtime.Intrinsics.Arm;
#endif

namespace BitFaster.Caching
{
    /// <summary>
    /// Represents a marker interface to enable instruction set hardware intrinsics.
    /// </summary>
    public interface IsaProbe
    {
        /// <summary>
        /// Gets a value indicating whether AVX2 is supported.
        /// </summary>
        bool IsAvx2Supported { get; }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Gets a value indicating whether Arm64 is supported.
        /// </summary>
        bool IsArm64Supported { get => false; }
#endif
    }

    /// <summary>
    /// Detect support for hardware instructions via intrinsics.
    /// </summary>
    public readonly struct DetectIsa : IsaProbe
    {
#if NETSTANDARD2_0
        /// <inheritdoc/>
        public bool IsAvx2Supported => false;
#else
        /// <inheritdoc/>
        public bool IsAvx2Supported => Avx2.IsSupported;
#endif

#if NET6_0_OR_GREATER
        /// <inheritdoc/>
        public bool IsArm64Supported => AdvSimd.Arm64.IsSupported;
#else
        /// <inheritdoc/>
        public bool IsArm64Supported => false;
#endif
    }

    /// <summary>
    /// Force disable hardware instructions via intrinsics.
    /// </summary>
    public readonly struct DisableHardwareIntrinsics : IsaProbe
    {
        /// <inheritdoc/>
        public bool IsAvx2Supported => false;

        /// <inheritdoc/>
        public bool IsArm64Supported => false;
    }
}
